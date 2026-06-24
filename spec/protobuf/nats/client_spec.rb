require "spec_helper"

describe ::Protobuf::Nats::Client do
  class ExampleServiceClass; end

  let(:service) { ExampleServiceClass }
  let(:method) { :created }
  let(:options) {
    {
      :service => service,
      :method => method
    }
  }

  subject { described_class.new(options) }

  describe "#ack_timeout" do
    it "can be set via the PB_NATS_CLIENT_ACK_TIMEOUT environment variable" do
      ::ENV["PB_NATS_CLIENT_ACK_TIMEOUT"] = "1000"

      expect(subject.ack_timeout).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_ACK_TIMEOUT")
    end

    it "has a default value" do
      expect(subject.ack_timeout).to eq(5)
    end
  end

  describe "#nack_backoff_intervals" do
    it "can be set via the PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS environment variable" do
      ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS"] = "10,20,30"

      expect(subject.nack_backoff_intervals).to eq([10, 20, 30])

      ::ENV.delete("PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS")
    end

    it "has a default value" do
      expect(subject.nack_backoff_intervals).to eq([0, 1, 3, 5, 10])
    end
  end

  describe "#nack_backoff_splay" do
    it "is a random value between zero and #nack_backoff_splay_limit" do
      allow(subject).to receive(:nack_backoff_splay_limit).and_return(100)
      allow(subject).to receive(:rand).with(100).and_return(33)
      expect(subject.nack_backoff_splay).to eq(33)
    end

    it "is always zero when #nack_backoff_splay_limit is zero" do
      allow(subject).to receive(:nack_backoff_splay_limit).and_return(0)
      expect(subject.nack_backoff_splay).to eq(0)
    end
  end

  describe "#nack_backoff_splay_limit" do
    it "can be set via the PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT environment variable" do
      ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT"] = "1000"

      expect(subject.nack_backoff_splay_limit).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT")
    end

    it "has a default value" do
      expect(subject.nack_backoff_splay_limit).to eq(10)
    end
  end

  describe "#reconnect_delay" do
    it "can be set via the PB_NATS_CLIENT_RECONNECT_DELAY environment variable" do
      ::ENV["PB_NATS_CLIENT_RECONNECT_DELAY"] = "1000"

      expect(subject.reconnect_delay).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_RECONNECT_DELAY")
    end

    it "defaults to the ack_timeout" do
      expect(subject.reconnect_delay).to eq(subject.ack_timeout)
    end
  end

  describe "#response_timeout" do
    it "can be set via the PB_NATS_CLIENT_RESPONSE_TIMEOUT environment variable" do
      ::ENV["PB_NATS_CLIENT_RESPONSE_TIMEOUT"] = "1000"

      expect(subject.response_timeout).to eq(1_000)

      ::ENV.delete("PB_NATS_CLIENT_RESPONSE_TIMEOUT")
    end

    it "has a default value" do
      expect(subject.response_timeout).to eq(60)
    end
  end

  describe "#cached_subscription_key" do
    it "caches the instance of a subscription key" do
      ::Protobuf::Nats::Client.subscription_key_cache.clear
      expect(::Protobuf::Nats).to receive(:subscription_key).once.and_call_original

      subject.cached_subscription_key
      subject.cached_subscription_key
    end
  end

  def inbox_muxer_reply_to(inbox, msg_token)
    "#{inbox}.#{msg_token}"
  end

  describe "#nats_request_with_two_responses" do
    let(:client) { ::FakeNatsClient.new }
    let(:msg_subject) { "rpc.yolo.brolo" }
    let(:ack) { ::Protobuf::Nats::Messages::ACK }
    let(:nack) { ::Protobuf::Nats::Messages::NACK }
    let(:response) { "final count down" }

    before do
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(client)

      # The RESPONSE_MUXER is a singleton that carries state between tests.
      # We must force it to restart so it subscribes to the new fake client
      # instance created for this test block.
      subject.response_muxer.restart

      ::Protobuf::Nats::Client.subscription_key_cache.clear
    end

    it "processes a request and returns the final response" do
      client.will_reply_with(ack, response)
      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
    end

    it "returns an :ack_timeout when the ack is not signaled" do
      # No reply is configured, so the client will time out waiting for an ACK.
      options = {:ack_timeout => 0.01, :timeout => 0.02}
      expect(subject.nats_request_with_two_responses(msg_subject, "request data", options)).to eq(:ack_timeout)
    end

    it "can send messages out of order and still complete" do
      client.will_reply_with(response, ack)
      server_response = subject.nats_request_with_two_responses(msg_subject, "request data", {})
      expect(server_response).to eq(response)
    end

    it "raises a response timeout when the ack is signaled but the pb response is not" do
      client.will_reply_with(ack)
      options = {:timeout => 0.01}
      expect { subject.nats_request_with_two_responses(msg_subject, "request data", options) }.to raise_error(::Protobuf::Nats::Errors::ResponseTimeout, "ExampleServiceClass#created")
    end

    it "returns :nack when the server responds with nack" do
      client.will_reply_with(nack)
      options = {:timeout => 0.01}
      expect(subject.nats_request_with_two_responses(msg_subject, "request data", options)).to eq(:nack)
    end
  end

  describe "#send_request" do
    let(:subscription_inbox) { ::Protobuf::Nats::Client::SubscriptionInbox.new(double("sub", :is_valid => true), "INBOX") }

    before do
      allow_any_instance_of(::Protobuf::Nats::Client).to receive(:new_subscription_inbox).and_return(subscription_inbox)
      # Keep retry jitter out of timing-sensitive assertions by default.
      # (allow_any_instance_of so we don't instantiate `subject` before the
      # per-test client_nats_connection stub, which would start the muxer early.)
      allow_any_instance_of(::Protobuf::Nats::Client).to receive(:reconnect_delay_splay).and_return(0)
    end

    it "retries 3 times when and raises a NATS timeout" do
      expect(subject).to receive(:setup_connection).exactly(3).times
      expect(subject).to receive(:nats_request_with_two_responses).and_return(:ack_timeout).exactly(3).times
      expect { subject.send_request }.to raise_error(::Protobuf::Nats::Errors::RequestTimeout, "ExampleServiceClass#created")
    end

    it "retries when the server responds with NACK" do
      allow(subject).to receive(:nack_backoff_splay).and_return(10)
      allow(subject).to receive(:nack_backoff_intervals).and_return([10, 20])
      # Expect sleep with the correct backoff values.
      expect(subject).to receive(:sleep).with((10 + 10) / 1000.0).ordered
      expect(subject).to receive(:sleep).with((20 + 10) / 1000.0).ordered
      # The loop will run 3 times before raising an error.
      expect(subject).to receive(:setup_connection).exactly(3).times
      # Stub the method to reliably return :nack.
      expect(subject).to receive(:nats_request_with_two_responses).exactly(3).times.and_return(:nack)
      # The final attempt will raise a timeout error.
      expect { subject.send_request }.to raise_error(::Protobuf::Nats::Errors::RequestTimeout, "ExampleServiceClass#created")
    end

    it "waits the reconnect_delay duration when the nats connection is reconnecting" do
      error = ::Protobuf::Nats::Errors::IOException.new
      client = ::FakeNatsClient.new
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(client)
      allow(client).to receive(:publish).and_raise(error)
      allow(subject).to receive(:setup_connection)
      expect(subject).to receive(:reconnect_delay).and_return(0.01).exactly(3).times
      expect { subject.send_request }.to raise_error(error)
    end

    # Regression: when jnats was dropped for nats-pure, the rescue only matched
    # the (never-raised) MriIOException, so a dropped connection escaped as an
    # immediate RPC_ERROR instead of being retried. These cover the errors the
    # pure-ruby client and socket layer actually raise on a broken connection.
    [
      ::EOFError.new("EOF"),
      ::IOError.new("stream closed"),
      ::Errno::ECONNRESET.new,
      ::Errno::EPIPE.new,
    ].each do |transport_error|
      it "retries and waits reconnect_delay on a #{transport_error.class} transport error" do
        client = ::FakeNatsClient.new
        allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(client)
        allow(client).to receive(:publish).and_raise(transport_error)
        allow(subject).to receive(:setup_connection)
        expect(subject).to receive(:reconnect_delay).and_return(0.01).exactly(3).times
        expect { subject.send_request }.to raise_error(transport_error.class)
      end
    end

    it "recovers after a single transient transport error and returns the response" do
      allow(subject).to receive(:setup_connection)
      allow(subject).to receive(:reconnect_delay).and_return(0.01)
      allow(subject).to receive(:parse_response) { subject.instance_variable_get(:@response_data) }
      call_count = 0
      allow(subject).to receive(:nats_request_with_two_responses) do
        call_count += 1
        raise ::Errno::ECONNRESET if call_count == 1
        "final count down"
      end

      expect(subject.send_request).to eq("final count down")
      expect(call_count).to eq(2)
    end

    it "adds jitter to the reconnect delay between transport retries" do
      allow(subject).to receive(:reconnect_delay_splay).and_call_original
      ::ENV["PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT"] = "1000"
      allow(subject).to receive(:reconnect_delay).and_return(0)
      allow(subject).to receive(:setup_connection)
      slept = []
      allow(subject).to receive(:sleep) { |s| slept << s }
      allow(subject).to receive(:nats_request_with_two_responses).and_raise(::Errno::ECONNRESET)

      expect { subject.send_request }.to raise_error(::Errno::ECONNRESET)
      # Jitter present (splay in [0,1)s) and bounded.
      expect(slept).to all(be_between(0, 1))
    ensure
      ::ENV.delete("PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT")
    end

    it "honors PB_NATS_CLIENT_MAX_RETRIES" do
      ::ENV["PB_NATS_CLIENT_MAX_RETRIES"] = "2"
      expect(subject).to receive(:setup_connection).exactly(2).times
      expect(subject).to receive(:nats_request_with_two_responses).and_return(:ack_timeout).exactly(2).times
      expect { subject.send_request }.to raise_error(::Protobuf::Nats::Errors::RequestTimeout)
    ensure
      ::ENV.delete("PB_NATS_CLIENT_MAX_RETRIES")
    end

    context "instrumentation" do
      it "instruments when a request times out" do
        allow(subject).to receive(:setup_connection)
        allow(subject).to receive(:nats_request_with_two_responses).and_return(:ack_timeout)
        event_triggered = false
        subscription = ::ActiveSupport::Notifications.subscribe("client.request_timeout.protobuf-nats") do
          event_triggered = true
        end
        subject.send_request rescue nil
        expect(event_triggered).to eq(true)
        ::ActiveSupport::Notifications.unsubscribe(subscription)
      end

      it "instruments when a request is nacked" do
        allow(subject).to receive(:setup_connection)
        allow(subject).to receive(:nats_request_with_two_responses).and_return(:nack)
        event_triggered = false
        subscription = ::ActiveSupport::Notifications.subscribe("client.request_nack.protobuf-nats") do
          event_triggered = true
        end
        subject.send_request rescue nil
        expect(event_triggered).to eq(true)
        ::ActiveSupport::Notifications.unsubscribe(subscription)
      end

      it "instruments the request duration" do
        allow(subject).to receive(:send_request_through_nats)
        event_triggered = false
        subscription = ::ActiveSupport::Notifications.subscribe("client.request_duration.protobuf-nats") do
          event_triggered = true
        end
        subject.send_request
        expect(event_triggered).to eq(true)
        ::ActiveSupport::Notifications.unsubscribe(subscription)
      end
    end
  end
end
