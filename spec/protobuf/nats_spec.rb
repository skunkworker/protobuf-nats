require "spec_helper"

describe ::Protobuf::Nats do
  it "has a version number" do
    expect(Protobuf::Nats::VERSION).not_to be nil
  end

  class ExampleServiceKlassBro; end

  it "can generate a correct subscription key" do
    expect(described_class.subscription_key(ExampleServiceKlassBro, :yolo_dude)).to eq("rpc.example_service_klass_bro.yolo_dude")
  end

  describe ".env_int" do
    after { ::ENV.delete("PB_NATS_TEST_INT") }

    it "returns the default when the var is unset" do
      expect(described_class.env_int("PB_NATS_TEST_INT", 5)).to eq(5)
    end

    it "parses a valid integer" do
      ::ENV["PB_NATS_TEST_INT"] = "42"
      expect(described_class.env_int("PB_NATS_TEST_INT", 5)).to eq(42)
    end

    it "falls back to the default (instead of 0) and logs on a malformed value" do
      ::ENV["PB_NATS_TEST_INT"] = "5s"
      expect(described_class.logger).to receive(:error).with(/malformed integer.*PB_NATS_TEST_INT/i)
      expect(described_class.env_int("PB_NATS_TEST_INT", 5)).to eq(5)
    end

    it "accepts a value at the minimum" do
      ::ENV["PB_NATS_TEST_INT"] = "1"
      expect(described_class.env_int("PB_NATS_TEST_INT", 5, :min => 1)).to eq(1)
    end

    it "falls back to the default and logs on a value below the minimum" do
      ::ENV["PB_NATS_TEST_INT"] = "0"
      expect(described_class.logger).to receive(:error).with(/out-of-range.*PB_NATS_TEST_INT/i)
      expect(described_class.env_int("PB_NATS_TEST_INT", 5, :min => 1)).to eq(5)
    end
  end

  describe ".env_float" do
    after { ::ENV.delete("PB_NATS_TEST_FLOAT") }

    it "returns the default when the var is unset" do
      expect(described_class.env_float("PB_NATS_TEST_FLOAT", 2.5)).to eq(2.5)
    end

    it "parses a valid float" do
      ::ENV["PB_NATS_TEST_FLOAT"] = "12.5"
      expect(described_class.env_float("PB_NATS_TEST_FLOAT", 2.5)).to eq(12.5)
    end

    it "falls back to the default (instead of 0.0) and logs on a malformed value" do
      ::ENV["PB_NATS_TEST_FLOAT"] = "5s"
      expect(described_class.logger).to receive(:error).with(/malformed number.*PB_NATS_TEST_FLOAT/i)
      expect(described_class.env_float("PB_NATS_TEST_FLOAT", 2.5)).to eq(2.5)
    end
  end

  describe ".disable_subscription_byte_limit!" do
    it "sets the byte limit to infinity when supported" do
      sub = ::NATS::Subscription.new
      described_class.disable_subscription_byte_limit!(sub)
      expect(sub.pending_bytes_limit).to eq(::Float::INFINITY)
    end

    it "is a no-op for objects without the accessor" do
      expect { described_class.disable_subscription_byte_limit!(Object.new) }.not_to raise_error
    end
  end

  describe "#on_error" do
    # Reset error callbacks.
    before { described_class.instance_variable_set(:@error_callbacks, nil) }
    after { described_class.instance_variable_set(:@error_callbacks, nil) }

    it "has a logger error handler" do
      expect(described_class.error_callbacks.size).to eq(1)
      expect(described_class).to receive(:log_error).with("test")
      described_class.notify_error_callbacks("test")
    end

    it "can have multiple error callbacks" do
      was_invoked = false
      described_class.on_error do |_error|
        was_invoked = true
      end
      expect(described_class.error_callbacks.size).to eq(2)
      described_class.notify_error_callbacks(::ArgumentError.new("test"))
      expect(was_invoked).to eq(true)
    end

    it "raises an argument error when a callback does not have arity of 1" do
      expect { described_class.on_error {} }.to raise_error(::ArgumentError)
    end
  end

  describe "#log_error" do
    let(:logger) { ::Logger.new(nil) }

    before do
      allow(::Protobuf::Logging).to receive(:logger).and_return(logger)
    end

    it "does not log an error with a backtrace" do
      expect(logger).to receive(:error).with("yolo")
      expect(logger).to_not receive(:error).with("")
      ::Protobuf::Nats.log_error(::ArgumentError.new("yolo"))
    end

    it "logs errors with backtrace" do
      error = ::ArgumentError.new("yolo")
      allow(error).to receive(:backtrace).and_return(["line 1", "line 2"])
      expect(logger).to receive(:error).with("yolo")
      expect(logger).to_not receive(:error).with("line 1\nline2")
      ::Protobuf::Nats.log_error(error)
    end
  end

  describe "#notify_error_callbacks" do
    it "logs an error raised by a callback" do
      the_error = ::RuntimeError.new("Save me from my callback sadness!")
      error_callback = lambda { |_error| fail the_error }
      allow(described_class).to receive(:error_callbacks).and_return([error_callback])

      expect(described_class).to receive(:log_error).with(the_error)
      described_class.notify_error_callbacks("yolo")
    end
  end

  describe "#notify_error_callbacks_async" do
    before { described_class.instance_variable_set(:@error_callbacks, nil) }
    after { described_class.instance_variable_set(:@error_callbacks, nil) }

    it "runs the callbacks off the calling thread" do
      delivered = ::Queue.new
      described_class.on_error { |e| delivered << e }

      described_class.notify_error_callbacks_async("boom")

      expect(::Timeout.timeout(2) { delivered.pop }).to eq("boom")
    end

    it "records a dropped error callback when the bounded executor is saturated" do
      before_count = described_class.error_callback_drop_count
      # Simulate the :discard fallback policy rejecting the job (queue full).
      allow(described_class::ERROR_CALLBACK_EXECUTOR).to receive(:post).and_return(false)
      expect(described_class).to receive(:instrument).with("error_callback_dropped", 1)

      described_class.notify_error_callbacks_async(::RuntimeError.new("flood"))

      expect(described_class.error_callback_drop_count).to eq(before_count + 1)
    end
  end

  # ActiveSupport re-raises a subscriber's exception to the instrument
  # caller. The gem instruments inside the request path, so a failing
  # subscriber must not change what the gem does.
  describe ".instrument" do
    let(:event) { "spec.instrument_probe" }
    let(:name) { "#{event}.protobuf-nats" }
    let!(:subscription) do
      ::ActiveSupport::Notifications.subscribe(name) { raise ::IOError, "statsd socket closed" }
    end

    after { ::ActiveSupport::Notifications.unsubscribe(subscription) }

    it "does not raise a subscriber error from the non-block form" do
      expect { described_class.instrument(event, 1) }.not_to raise_error
    end

    it "returns the block result when a subscriber raises" do
      expect(described_class.instrument(event) { :work_done }).to eq(:work_done)
    end

    it "runs the block once when a subscriber raises" do
      runs = 0
      described_class.instrument(event) { runs += 1 }
      expect(runs).to eq(1)
    end

    it "still raises an error from the block itself" do
      expect {
        described_class.instrument(event) { raise ::ArgumentError, "the real work failed" }
      }.to raise_error(::ArgumentError, "the real work failed")
    end

    it "runs the block when a subscriber raises in #start, before the block" do
      start_raiser = ::Object.new
      def start_raiser.start(*)
        raise ::IOError, "start failed"
      end

      def start_raiser.finish(*)
      end
      start_subscription = ::ActiveSupport::Notifications.subscribe(name, start_raiser)

      expect(described_class.instrument(event) { :work_done }).to eq(:work_done)
    ensure
      ::ActiveSupport::Notifications.unsubscribe(start_subscription)
    end

    it "counts subscriber errors and logs the first one" do
      described_class::SUBSCRIBER_ERROR_COUNT.value = 0
      expect(described_class.logger).to receive(:error).with(/subscriber for spec\.instrument_probe\.protobuf-nats.*IOError: statsd socket closed/).once

      3.times { described_class.instrument(event) }

      expect(described_class.subscriber_error_count).to eq(3)
    end
  end

  describe ".initial_connect" do
    let(:config) { described_class.config }
    let(:fake_nats) { ::FakeNatsClient.new }

    after do
      config.max_reconnect_attempts = ::Protobuf::Nats::Config::DEFAULTS[:max_reconnect_attempts]
      config.connection_options(true)
    end

    def budget_at_connect
      budget = nil
      allow(fake_nats).to receive(:connect).and_wrap_original do |original, opts|
        budget = opts[:max_reconnect_attempts]
        original.call(opts)
      end
      described_class.initial_connect(fake_nats)
      budget
    end

    it "connects with a budget of 1, then restores the configured budget" do
      expect(budget_at_connect).to eq(1)
      expect(fake_nats.options[:max_reconnect_attempts]).to eq(60_000)
    end

    it "keeps a configured budget below 1" do
      config.max_reconnect_attempts = 0
      config.connection_options(true)
      expect(budget_at_connect).to eq(0)
      expect(fake_nats.options[:max_reconnect_attempts]).to eq(0)
    end

    it "bounds 'reconnect forever' (-1) on the first connect only" do
      config.max_reconnect_attempts = -1
      config.connection_options(true)
      expect(budget_at_connect).to eq(1)
      expect(fake_nats.options[:max_reconnect_attempts]).to eq(-1)
    end

    it "does not replace a budget that nats-pure took from NATS_MAX_RECONNECT_ATTEMPTS" do
      allow(fake_nats).to receive(:connect) { fake_nats.options[:max_reconnect_attempts] = 7 }
      described_class.initial_connect(fake_nats)
      expect(fake_nats.options[:max_reconnect_attempts]).to eq(7)
    end

    it "does not change the shared connection options" do
      described_class.initial_connect(fake_nats)
      expect(config.connection_options[:max_reconnect_attempts]).to eq(60_000)
    end
  end

  describe "#start_client_nats_connection" do
    around do |example|
      previous = described_class.client_nats_connection
      described_class.client_nats_connection = nil
      described_class.instance_variable_set(:@last_connect_failure, nil)
      example.run
      described_class.instance_variable_set(:@last_connect_failure, nil)
      described_class.client_nats_connection = previous
    end

    it "connects with the connection options, a small first-connect budget, and no dead :disable_reconnect_buffer" do
      # spec_helper stubs this to a no-op by default; run the real thing here.
      allow(described_class).to receive(:start_client_nats_connection).and_call_original

      fake_nats = ::FakeNatsClient.new
      options_at_connect = nil
      allow(::Protobuf::Nats::NatsClient).to receive(:new).and_return(fake_nats)
      allow(fake_nats).to receive(:connect).and_wrap_original do |original, opts|
        options_at_connect = opts.dup
        original.call(opts)
      end
      # Stub the rest of the connection lifecycle calls.
      %i[flush on_disconnect on_reconnect on_close on_error].each do |m|
        allow(fake_nats).to receive(m)
      end

      described_class.start_client_nats_connection

      configured = described_class.config.connection_options
      expect(options_at_connect).to eq(configured.merge(:max_reconnect_attempts => 1))
      expect(options_at_connect).not_to have_key(:disable_reconnect_buffer)
      # Later reconnects get the configured budget back.
      expect(fake_nats.options[:max_reconnect_attempts]).to eq(configured[:max_reconnect_attempts])
    end

    # With NATS down, the Nth caller that waited on GET_CONNECTED_MUTEX ran
    # its own connect after the one before it failed, so it failed after N
    # connect attempts.
    it "fails callers that waited on a failed connect at once, without a connect of their own" do
      allow(described_class).to receive(:start_client_nats_connection).and_call_original

      connects = ::Concurrent::AtomicFixnum.new(0)
      first_connect_started = ::Queue.new
      release_first_connect = ::Queue.new
      allow(::Protobuf::Nats::NatsClient).to receive(:new) do
        fake_nats = ::FakeNatsClient.new
        allow(fake_nats).to receive(:connect) do
          if connects.increment == 1
            first_connect_started << true
            release_first_connect.pop
          end
          raise ::Errno::ECONNREFUSED
        end
        allow(fake_nats).to receive(:close)
        fake_nats
      end

      first = ::Thread.new { described_class.start_client_nats_connection rescue $! }
      first_connect_started.pop
      waiters = 3.times.map { ::Thread.new { described_class.start_client_nats_connection rescue $! } }
      wait_until { waiters.all? { |t| t.status == "sleep" } }
      release_first_connect << true

      expect(first.value).to be_a(::Errno::ECONNREFUSED)
      waiters.map(&:value).each do |error|
        expect(error).to be_a(::Protobuf::Nats::Errors::ConnectionFailed)
        expect(error.cause).to be_a(::Errno::ECONNREFUSED)
      end
      expect(connects.value).to eq(1)

      # A caller that arrives after the failure tries again.
      expect { described_class.start_client_nats_connection }.to raise_error(::Errno::ECONNREFUSED)
      expect(connects.value).to eq(2)
    end

    # A real nats-pure client against a closed port. At the gem default
    # (60,000 reconnect attempts) this first connect looped for hours.
    context "with a real nats-pure client and an unreachable server" do
      let(:config) { described_class.config }

      before do
        server = ::TCPServer.new("127.0.0.1", 0)
        closed_port = server.addr[1]
        server.close
        config.servers = ["nats://127.0.0.1:#{closed_port}"]
        config.reconnect_time_wait = 0.2
        config.connection_options(true)
      end

      after do
        config.servers = nil
        config.reconnect_time_wait = nil
        config.connection_options(true)
      end

      # At the gem default (60,000 reconnect attempts) this looped for hours.
      # The error class depends on the platform (ECONNREFUSED on JRuby,
      # EINVAL from setsockopt on CRuby/macOS), so check only the time.
      it "gives up the first connect in about one reconnect_time_wait" do
        allow(described_class).to receive(:start_client_nats_connection).and_call_original

        started_at = described_class.monotonic_time
        expect { described_class.start_client_nats_connection }.to raise_error(::SystemCallError)
        expect(described_class.monotonic_time - started_at).to be < 2
      end
    end

    it "closes the half-open client and does not cache the connection when the handshake fails" do
      allow(described_class).to receive(:start_client_nats_connection).and_call_original

      fake_nats = ::FakeNatsClient.new
      allow(::Protobuf::Nats::NatsClient).to receive(:new).and_return(fake_nats)
      %i[on_disconnect on_reconnect on_close on_error connect].each do |m|
        allow(fake_nats).to receive(m)
      end
      allow(fake_nats).to receive(:flush).and_raise(::NATS::IO::Timeout)
      # The half-open client must be closed so its reader/flusher threads don't leak.
      expect(fake_nats).to receive(:close)

      expect { described_class.start_client_nats_connection }.to raise_error(::NATS::IO::Timeout)
      expect(described_class.client_nats_connection).to be_nil
    end

    it "drops the cached connection when it closes so the next call rebuilds" do
      allow(described_class).to receive(:start_client_nats_connection).and_call_original

      fake_nats = ::FakeNatsClient.new
      allow(::Protobuf::Nats::NatsClient).to receive(:new).and_return(fake_nats)
      %i[on_disconnect on_reconnect on_error connect flush].each { |m| allow(fake_nats).to receive(m) }

      # Capture the on_close callback the lifecycle registers so we can fire it.
      close_callback = nil
      allow(fake_nats).to receive(:on_close) { |&blk| close_callback = blk }

      described_class.start_client_nats_connection
      expect(described_class.client_nats_connection).to eq(fake_nats)

      # nats-pure fires on_close when the connection terminally closes.
      close_callback.call

      expect(described_class.client_nats_connection).to be_nil
    end
  end
end
