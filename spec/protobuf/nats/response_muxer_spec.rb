require "spec_helper"
require "thread"

describe ::Protobuf::Nats::ResponseMuxer do
  let(:nats_client) { ::FakeNatsClient.new }
  subject { described_class.new }

  before do
    allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(nats_client)
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
end
