require "spec_helper"

describe ::Protobuf::Nats::Server do
  class SomeRandom < ::Protobuf::Message; end
  class SomeRandomService < ::Protobuf::Rpc::Service
    rpc :implemented, SomeRandom, SomeRandom
    rpc :implemented_again, SomeRandom, SomeRandom
    rpc :not_implemented, SomeRandom, SomeRandom
    def implemented; end
    def implemented_again; end
  end

  let(:logger) { ::Logger.new(nil) }
  let(:client) { ::FakeNatsClient.new }
  let(:options) {
    {
      :threads => 2,
      :client  => client,
      :server  => 'derpaderp'
    }
  }

  subject { described_class.new(options) }

  before do
    allow(::Protobuf::Logging).to receive(:logger).and_return(logger)
    allow(subject).to receive(:service_klasses).and_return([SomeRandomService])
  end

  describe "#instrument_thread_pool_sizes" do
    it "instruments the thread pool enqueued size" do
      enqueued_size = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_enqueued_size.protobuf-nats" do |_, _, _, _, size|
        enqueued_size = size
      end

      subject.instrument_thread_pool_sizes
      expect(enqueued_size).to_not eq(nil)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments the thread pool max size" do
      max_size = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_max_size.protobuf-nats" do |_, _, _, _, size|
        max_size = size
      end

      subject.instrument_thread_pool_sizes
      expect(max_size).to_not eq(nil)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments the thread pool running size" do
      running_size = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_running_size.protobuf-nats" do |_, _, _, _, size|
        running_size = size
      end

      subject.instrument_thread_pool_sizes
      expect(running_size).to_not eq(nil)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end
  end

  describe "#detect_and_handle_a_pause" do
    it "unsubscribes when the server is paused" do
      allow(subject).to receive(:paused?).and_return(true)
      expect(subject).to receive(:unsubscribe)
      subject.detect_and_handle_a_pause
    end

    it "subscribes and restarts slow start when the pause file is removed" do
      subject.instance_variable_set(:@processing_requests, false)
      expect(subject).to receive(:subscribe)
      subject.detect_and_handle_a_pause
    end

    it "never calls unsubscribe more than once per pause" do
      allow(subject).to receive(:paused?).and_return(true)
      expect(subject).to receive(:unsubscribe).once
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
    end
    it "never calls subscribe more than once per pause" do
      subject.instance_variable_set(:@processing_requests, false)
      expect(subject).to receive(:subscribe).once
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
      subject.detect_and_handle_a_pause
    end
  end

  describe "#max_queue_size" do
    it "can be set via options hash" do
      expect(subject.max_queue_size).to eq(2)
    end

    it "can be set via PB_NATS_SERVER_MAX_QUEUE_SIZE environment variable" do
      ::ENV["PB_NATS_SERVER_MAX_QUEUE_SIZE"] = "10"

      expect(subject.max_queue_size).to eq(10)

      ::ENV.delete("PB_NATS_SERVER_MAX_QUEUE_SIZE")
    end
  end

  describe "pause_file_path" do
    it "is nil by default" do
      expect(subject.pause_file_path).to eq(nil)
    end

    it "can be set via PB_NATS_SERVER_PAUSE_FILE_PATH environment variable" do
      ::ENV["PB_NATS_SERVER_PAUSE_FILE_PATH"] = "/tmp/rpc-paused-bro"

      expect(subject.pause_file_path).to eq("/tmp/rpc-paused-bro")

      ::ENV.delete("PB_NATS_SERVER_PAUSE_FILE_PATH")
    end
  end

  describe "#paused?" do
    let(:test_file) { "#{::SecureRandom.uuid}-testing-123" }
    # Ensure the test file is always cleaned up.
    after { ::File.delete(test_file) if ::File.exist?(test_file) }

    it "pauses when a pause file is set" do
      ::ENV["PB_NATS_SERVER_PAUSE_FILE_PATH"] = test_file
      expect(subject).to_not be_paused
      ::File.write(test_file, "")
      expect(subject).to be_paused
      ::ENV.delete("PB_NATS_SERVER_PAUSE_FILE_PATH")
    end
  end

  describe "#slow_start_delay" do
    it "has a default" do
      expect(subject.slow_start_delay).to eq(10)
    end

    it "can be set via PB_NATS_SERVER_SLOW_START_DELAY environment variable" do
      ::ENV["PB_NATS_SERVER_SLOW_START_DELAY"] = "20"

      expect(subject.slow_start_delay).to eq(20)

      ::ENV.delete("PB_NATS_SERVER_SLOW_START_DELAY")
    end
  end

  describe "#subscriptions_per_rpc_endpoint" do
    it "has a default" do
      expect(subject.subscriptions_per_rpc_endpoint).to eq(10)
    end

    it "can be set via PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT environment variable" do
      ::ENV["PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT"] = "20"

      expect(subject.subscriptions_per_rpc_endpoint).to eq(20)

      ::ENV.delete("PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT")
    end
  end

  describe "#subscribe_to_services_once" do
    context "do not subscribe to when includes any of" do
      it "subscribes to services when they are not present" do
        config = ::Protobuf::Nats.config

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented", "rpc.some_random_service.implemented_again"])
      end

      it "does not subscribe when an included substring is present for an implemented service" do
        config = ::Protobuf::Nats.config
        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of << "random_service"

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq([])

        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.clear
      end

      it "does not subscribe when an included substring is present for an implemented service (and in only group)" do
        config = ::Protobuf::Nats.config
        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of << "random_service"
        config.server_subscription_key_only_subscribe_to_when_includes_any_of << "random_service"

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq([])

        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.clear
        config.server_subscription_key_only_subscribe_to_when_includes_any_of.clear
      end
    end

    context "only subscribe to when includes any of" do
      it "subscribes to services when they are not present" do
        config = ::Protobuf::Nats.config

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented", "rpc.some_random_service.implemented_again"])
      end

      it "subscribes when an included substring is present for an implemented service and restrains possible" do
        config = ::Protobuf::Nats.config
        config.server_subscription_key_only_subscribe_to_when_includes_any_of << "again"

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented_again"])

        config.server_subscription_key_only_subscribe_to_when_includes_any_of.clear
      end

      it "subscribes when an included substring is present for an implemented service" do
        config = ::Protobuf::Nats.config
        config.server_subscription_key_only_subscribe_to_when_includes_any_of << "random_service"

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented", "rpc.some_random_service.implemented_again"])

        config.server_subscription_key_only_subscribe_to_when_includes_any_of.clear
      end

      it "does not subscribe when an included substring is present for an implemented service (and in do not group)" do
        config = ::Protobuf::Nats.config
        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of << "random_service"
        config.server_subscription_key_only_subscribe_to_when_includes_any_of << "random_service"

        subject.subscribe_to_services_once
        expect(client.subscriptions.keys).to eq([])

        config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.clear
        config.server_subscription_key_only_subscribe_to_when_includes_any_of.clear
      end
    end

    it "subscribes to services that inherit from protobuf rpc service" do
      subject.subscribe_to_services_once
      expect(client.subscriptions.keys).to eq(["rpc.some_random_service.implemented", "rpc.some_random_service.implemented_again"])
    end
  end

  describe "#enqueue_request" do
    it "returns false when the thread pool and thread pool queue is full and publish NACK" do
      # Fill the thread pool.
      2.times { subject.thread_pool.push { sleep 1 } }
      # Fill the thread pool queue.
      2.times { subject.thread_pool.push { sleep 1 } }

      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::NACK)
      expect(subject.enqueue_request("", "inbox_123")).to eq(false)
    end

    it "logs a thread pool is full error when subscription manager processes a message but the thread pool is full" do
      # Fill the thread pool and its queue.
      2.times { subject.thread_pool.push { sleep 1 } }
      2.times { subject.thread_pool.push { sleep 1 } }

      # Expect NACK to be published when enqueue_request is called
      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::NACK)

      # Expect the logger to log a thread pool is full error
      expect(logger).to receive(:error) do |&block|
        expect(block.call).to match(/Thread pool is full! Dropping message for subject: rpc.some_subject/)
      end

      # Deliver the message by putting it into subscription manager's queue
      message = double(:data => "req_data", :reply => "inbox_123", :subject => "rpc.some_subject")
      pending_queue = subject.subscription_manager.instance_variable_get(:@pending_queue)
      pending_queue.push(message)

      # Give the subscription manager thread a tiny bit of time to pop and execute
      sleep 0.1

      # Cleanup
      subject.thread_pool.kill
      subject.subscription_manager.shutdown(0.1)
    end

    it "sends an ACK if the thread pool enqueued the task" do
      # Fill the thread pool.
      2.times { subject.thread_pool.push { sleep 1 } }
      expect(subject.nats).to receive(:publish).with("inbox_123", ::Protobuf::Nats::Messages::ACK)
      # Wait for promise to finish executing.
      expect(subject.enqueue_request("", "inbox_123")).to eq(true)
      subject.thread_pool.kill
    end

    it "logs any error that is raised within the request block" do
      request_data = "yolo"
      expect(subject).to receive(:handle_request).with(request_data, 'server' => 'derpaderp').and_raise(::RuntimeError, "mah error")
      expect(logger).to receive(:error).once.ordered.with("mah error")
      expect(logger).to receive(:error).once.ordered.with("RuntimeError")
      expect(logger).to receive(:error).once.ordered

      # Wait for promise to finish executing.
      expect(subject.enqueue_request(request_data, "inbox_123")).to eq(true)
      sleep 0.1 until subject.thread_pool.size.zero?
    end

    it "returns an ACK and a response" do
      response = "some response data"
      inbox = "inbox_123"
      expect(subject).to receive(:handle_request).and_return(response)
      expect(client).to receive(:publish).once.ordered.with(inbox, ::Protobuf::Nats::Messages::ACK)
      expect(client).to receive(:publish).once.ordered.with(inbox, response)

      # Wait for promise to finish executing.
      expect(subject.enqueue_request("", inbox)).to eq(true)
      sleep 0.1 until subject.thread_pool.size.zero?
    end
  end

  describe "instrumentation" do
    it "instruments the thread pool execution delay" do
      expect(subject).to receive(:handle_request).and_return("response")
      execution_delay = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_execution_delay.protobuf-nats" do |_, _, _, _, delay|
        execution_delay = delay
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(execution_delay).to_not eq(nil)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instrument a request duration" do
      expect(subject).to receive(:handle_request) do
        sleep 0.05
        "response"
      end
      request_duration = nil
      subscription = ::ActiveSupport::Notifications.subscribe "server.request_duration.protobuf-nats" do |_, _, _, _, duration|
        request_duration = duration
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(request_duration).to be >= 0.05
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments when a message received" do
      allow(subject.thread_pool).to receive(:push)
      message_was_received = false
      subscription = ::ActiveSupport::Notifications.subscribe "server.message_received.protobuf-nats" do
        message_was_received = true
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(message_was_received).to eq(true)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end

    it "instruments when a message dropped" do
      allow(subject.thread_pool).to receive(:push).and_return(false)
      message_was_dropped = false
      subscription = ::ActiveSupport::Notifications.subscribe "server.message_dropped.protobuf-nats" do
        message_was_dropped = true
      end

      subject.enqueue_request("", "YOLO123")
      sleep 0.1 until subject.thread_pool.size.zero?

      expect(message_was_dropped).to eq(true)
      ::ActiveSupport::Notifications.unsubscribe(subscription)
    end
  end

  describe "edge cases and fixes" do
    describe "#running?" do
      it "returns true when server is running" do
        expect(subject.instance_variable_get(:@stopped)).to be(false)
        expect(subject.running?).to be(true)
      end

      it "returns false when server is stopped" do
        subject.instance_variable_set(:@stopped, true)
        expect(subject.running?).to be(false)
      end
    end

    describe "ACK/NACK error handling" do
      it "handles NATS publish errors when sending ACK" do
        allow(subject.thread_pool).to receive(:push).and_return(true)
        allow(client).to receive(:publish).and_raise(StandardError, "NATS disconnected")

        # Expect error to be logged
        expect(logger).to receive(:error).at_least(:once)

        # Should not raise, just log
        expect { subject.enqueue_request("data", "reply123") }.not_to raise_error
      end

      it "handles NATS publish errors when sending NACK" do
        allow(subject.thread_pool).to receive(:push).and_return(false)
        allow(client).to receive(:publish).and_raise(StandardError, "NATS disconnected")

        # Expect error to be logged
        expect(logger).to receive(:error).at_least(:once)

        # Should not raise, just log
        expect { subject.enqueue_request("data", "reply123") }.not_to raise_error
      end
    end

    describe "#finish_slow_start" do
      before do
        allow(subject).to receive(:subscribe_to_services_once)
        allow(subject).to receive(:sleep)
      end

      it "logs successful completion" do
        # Allow any info logs, then verify the specific one was called
        allow(logger).to receive(:info)
        subject.finish_slow_start
        expect(logger).to have_received(:info).with(/slow start finished successfully/i)
      end

      it "exits early and logs when server is stopping" do
        # Stop after first iteration
        allow(subject).to receive(:slow_start_delay).and_return(0)
        call_count = 0
        allow(subject).to receive(:subscribe_to_services_once) do
          call_count += 1
          subject.instance_variable_set(:@running, false) if call_count == 1
        end

        expect(logger).to receive(:info).with(/slow start interrupted.*stopping/i)
        expect(logger).not_to receive(:info).with(/finished successfully/i)

        subject.finish_slow_start
      end

      it "exits early and logs when server is paused" do
        allow(subject).to receive(:paused?).and_return(false, true)
        allow(subject).to receive(:slow_start_delay).and_return(0)

        expect(logger).to receive(:info).with(/slow start interrupted.*paused/i)
        expect(logger).not_to receive(:info).with(/finished successfully/i)

        subject.finish_slow_start
      end
    end

    describe "#detect_and_handle_a_pause" do
      it "is thread-safe with mutex" do
        # Verify mutex exists
        expect(subject.instance_variable_get(:@pause_mutex)).to be_a(Mutex)

        # Simulate concurrent calls
        threads = 10.times.map do
          Thread.new { subject.detect_and_handle_a_pause }
        end

        threads.each(&:join)

        # No exceptions should be raised
      end

      it "handles pause/resume transitions safely" do
        allow(subject).to receive(:paused?).and_return(true)
        allow(subject).to receive(:unsubscribe)

        # First call should unsubscribe
        subject.detect_and_handle_a_pause
        expect(subject.instance_variable_get(:@processing_requests)).to be(false)

        # Resume
        allow(subject).to receive(:paused?).and_return(false)
        allow(subject).to receive(:subscribe)

        subject.detect_and_handle_a_pause
        expect(subject.instance_variable_get(:@processing_requests)).to be(true)
      end
    end

    describe "shutdown sequence" do
      before do
        # Stub NATS callback methods
        allow(client).to receive(:on_reconnect)
        allow(client).to receive(:on_disconnect)
        allow(client).to receive(:on_error)
        allow(client).to receive(:on_close)
        allow(client).to receive(:close)
      end

      it "closes NATS connection on shutdown" do
        # Mock the run loop to exit immediately without sleeping
        allow(subject).to receive(:loop)
        allow(subject).to receive(:print_subscription_keys)
        allow(subject).to receive(:subscribe)
        allow(subject).to receive(:unsubscribe)

        # Expect NATS to be closed
        expect(client).to receive(:close)

        # Stop immediately - no need for thread and sleep
        subject.instance_variable_set(:@running, false)
        subject.run
      end

      it "handles subscription manager shutdown timeout" do
        # Mock the run loop to exit immediately
        allow(subject).to receive(:loop)
        allow(subject).to receive(:print_subscription_keys)
        allow(subject).to receive(:subscribe)
        allow(subject).to receive(:unsubscribe)

        # Make shutdown hang (but Timeout will catch it in 10 seconds, which is mocked)
        allow(subject.subscription_manager).to receive(:shutdown) { sleep 100 }

        # Stub Timeout to trigger immediately instead of waiting 10 seconds
        allow(Timeout).to receive(:timeout).with(10).and_raise(Timeout::Error)

        # Allow any error logs
        allow(logger).to receive(:error)
        allow(logger).to receive(:info)
        allow(logger).to receive(:warn)

        subject.instance_variable_set(:@running, false)
        subject.run

        # Verify the error was logged
        expect(logger).to have_received(:error).with(/subscription manager shutdown timed out/i)
      end

      it "handles thread pool shutdown timeout" do
        # Mock the run loop to exit immediately
        allow(subject).to receive(:loop)
        allow(subject).to receive(:print_subscription_keys)
        allow(subject).to receive(:subscribe)
        allow(subject).to receive(:unsubscribe)

        # Make thread pool wait return false immediately (simulating timeout)
        allow(subject.thread_pool).to receive(:shutdown)
        allow(subject.thread_pool).to receive(:wait_for_termination).and_return(false)

        # Allow any logs
        allow(logger).to receive(:warn)
        allow(logger).to receive(:info)

        # Should instrument the timeout
        timeout_instrumented = false
        subscription = ::ActiveSupport::Notifications.subscribe "server.thread_pool_shutdown_timeout.protobuf-nats" do
          timeout_instrumented = true
        end

        subject.instance_variable_set(:@running, false)
        subject.run

        expect(timeout_instrumented).to be(true)
        expect(logger).to have_received(:warn).with(/thread pool did not shut down cleanly/i)
        ::ActiveSupport::Notifications.unsubscribe(subscription)
      end
    end

    describe "typo fixes" do
      it "spells 'Publishing' correctly in log" do
        allow(subject.thread_pool).to receive(:push).and_yield.and_return(true)
        allow(subject).to receive(:handle_request).and_return("response")
        allow(client).to receive(:publish)

        # Allow any debug logs
        allow(logger).to receive(:debug)

        subject.enqueue_request("data", "reply123")

        # Verify the correct spelling was used
        expect(logger).to have_received(:debug).with(/Publishing response/i)
      end
    end
  end
end
