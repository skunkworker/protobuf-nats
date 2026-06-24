require "spec_helper"

describe ::Protobuf::Nats do
  it "has a version number" do
    expect(Protobuf::Nats::VERSION).not_to be nil
  end

  class ExampleServiceKlassBro; end

  it "can generate a correct subscription key" do
    expect(described_class.subscription_key(ExampleServiceKlassBro, :yolo_dude)).to eq("rpc.example_service_klass_bro.yolo_dude")
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

  describe "#start_client_nats_connection" do
    around do |example|
      previous = described_class.client_nats_connection
      described_class.client_nats_connection = nil
      example.run
      described_class.client_nats_connection = previous
    end

    it "connects with the unmodified connection options (no dead :disable_reconnect_buffer)" do
      # spec_helper stubs this to a no-op by default; run the real thing here.
      allow(described_class).to receive(:start_client_nats_connection).and_call_original

      fake_nats = ::FakeNatsClient.new
      received_options = nil
      allow(::Protobuf::Nats::NatsClient).to receive(:new).and_return(fake_nats)
      allow(fake_nats).to receive(:connect) { |opts| received_options = opts }
      # Stub the rest of the connection lifecycle calls.
      %i[flush on_disconnect on_reconnect on_close on_error].each do |m|
        allow(fake_nats).to receive(m)
      end

      described_class.start_client_nats_connection

      expect(received_options).to eq(described_class.config.connection_options)
      expect(received_options).not_to have_key(:disable_reconnect_buffer)
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
