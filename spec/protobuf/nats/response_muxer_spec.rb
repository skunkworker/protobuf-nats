require "spec_helper"
require "thread"

describe ::Protobuf::Nats::ResponseMuxer do
  let(:nats_client) { ::FakeNatsClient.new }
  subject { described_class.new }

  before do
    allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(nats_client)
    # Stub unsubscribe on the fake subscriptions so they don't crash with NoMethodError on nil @nc
    allow_any_instance_of(::NATS::Subscription).to receive(:unsubscribe)
    # Use a real logger but stub its output device so we can spy on it
    # without generating log noise during tests.
    logger = ::Logger.new(nil)
    allow(subject).to receive(:logger).and_return(logger)
  end

  describe "#start" do
    it "does not start if the nats client connection is nil" do
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(nil)
      subject.start
      expect(subject.started?).to be(false)
    end

    context "with a running thread" do
      let(:subscription) { nats_client.subscribe("test.subscription") }
      let(:queue) { subscription.pending_queue }

      it "logs a per-message error and continues processing" do
        allow(nats_client).to receive(:subscribe).and_return(subscription)
        bad_message = double(:subject => nil, :data => "bar")
        allow(queue).to receive(:pop).and_return(bad_message, nil)
        expect(subject.logger).to receive(:error).with(/failed to process a message/i).once

        subject.send(:start)
        handler_thread = subject.instance_variable_get(:@resp_handlers).first
        sleep 0.1 # Give thread time to run, pop, and hit the rescue block.
        expect(handler_thread.alive?).to be(true)
        handler_thread.kill
      end

      it "logs a fatal error and attempts to restart" do
        start_calls = 0
        mutex = Mutex.new

        allow(nats_client).to receive(:subscribe).and_return(subscription)

        pop_has_raised = false
        allow(queue).to receive(:pop) do
          if !pop_has_raised
            pop_has_raised = true
            raise ::ThreadError, "Queue closed"
          else
            # On subsequent calls from the restarted thread, return nil.
            # The muxer loop handles nil and just continues.
            nil
          end
        end

        # Wrap the original start method to count calls.
        original_start = subject.method(:start)
        allow(subject).to receive(:start) do
          mutex.synchronize { start_calls += 1 }
          original_start.call
        end

        # Expectations for recovery
        expect(subject.logger).to receive(:error).with(/thread crashed fatally/i)
        expect(subject.logger).to receive(:warn).with(/waiting 1s before attempting to restart/i)
        expect(subject).to receive(:sleep).with(1)

        # Action: Start the muxer.
        subject.send(:start)

        # Wait until start has been called twice.
        retries = 0

        until mutex.synchronize { start_calls } >= 2 || retries > 20 # 2 seconds
          sleep 0.1
          retries += 1
        end

        expect(mutex.synchronize { start_calls }).to be >= 2
      end
    end
  end

  describe "edge cases and vulnerabilities" do
    describe "lock mismatch on restart" do
      it "allows calling next_message without ThreadError after restart" do
        subject.start
        req = subject.new_request
        subject.restart
        # In a healthy implementation, next_message should just wait (and timeout),
        # but NOT raise a ThreadError due to lock mismatch.
        expect { req.next_message(0.01) }.to raise_error(::NATS::Timeout)
      end
    end

    describe "missing unsubscription" do
      it "unsubscribes from the old subscription when restarted" do
        subject.start
        old_sub = subject.instance_variable_get(:@resp_sub)
        expect(old_sub).to receive(:unsubscribe).once
        subject.restart
      end
    end

    describe "unstarted / failed start state" do
      it "does not raise NoMethodError on nil when calling new_request before start" do
        expect { subject.new_request }.not_to raise_error(NoMethodError)
      end

      it "does not raise NoMethodError on nil when calling cleanup before start" do
        expect { subject.cleanup("token") }.not_to raise_error(NoMethodError)
      end
    end

    describe "dead thread accumulation" do
      it "does not accumulate dead threads in @resp_handlers during self-healing/restarts" do
        subject.start
        original_handler = subject.instance_variable_get(:@resp_handlers).first
        expect(original_handler).to be_alive

        # Kill the handler to make it dead
        original_handler.kill
        sleep 0.05
        expect(original_handler).not_to be_alive

        # Trigger restart
        subject.restart

        handlers = subject.instance_variable_get(:@resp_handlers)
        expect(handlers.any? { |t| !t.alive? }).to be(false)
      end
    end
  end
end
