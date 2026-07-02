require "spec_helper"

# Full-stack integration against a real NATS server (see spec_helper for how
# these are gated). Exercises the actual wire protocol: client publish with a
# muxer reply inbox -> server intake -> ACK -> thread-pool handler -> response.
class IntegrationPing < ::Protobuf::Message
  optional :string, :payload, 1
end

class IntegrationEchoService < ::Protobuf::Rpc::Service
  rpc :echo, IntegrationPing, IntegrationPing

  def echo
    respond_with ::IntegrationPing.new(:payload => request.payload)
  end
end

describe "protobuf-nats against a real NATS server", :integration => true do
  def build_request_data(payload)
    ::Protobuf::Socketrpc::Request.new(
      :service_name => "IntegrationEchoService",
      :method_name => "echo",
      :request_proto => ::IntegrationPing.new(:payload => payload).encode,
      :caller => "integration-spec"
    ).encode
  end

  def new_client
    ::Protobuf::Nats::Client.new(:service => IntegrationEchoService, :method => :echo)
  end

  # Drives a real RPC and returns the echoed payload. Retries NACKs (the
  # server's real backpressure signal when its 2-thread pool saturates) the
  # same way the production client loop does.
  def rpc(client, payload)
    opts = { :ack_timeout => 5, :timeout => 10 }
    request_data = build_request_data(payload)
    data = nil
    30.times do
      data = client.nats_request_with_two_responses(client.cached_subscription_key, request_data, opts)
      break unless data == :nack
      sleep 0.05
    end
    raise "request did not complete: #{data.inspect}" if data.is_a?(::Symbol)

    response = ::Protobuf::Socketrpc::Response.decode(data)
    raise "rpc error: #{response.error}" unless response.error.to_s.empty?
    ::IntegrationPing.decode(response.response_proto).payload
  end

  before(:all) do
    ::Protobuf::Nats.config.servers = ["nats://#{PB_NATS_INTEGRATION_HOST}:#{PB_NATS_INTEGRATION_PORT}"]
    ::Protobuf::Nats.config.connection_options(true)

    @server_nats = ::Protobuf::Nats::NatsClient.new
    @server = ::Protobuf::Nats::Server.new(:threads => 2, :client => @server_nats, :server => "integration-spec")
    # One round of subscriptions is enough; skip Server#run's slow start /
    # supervision loop, which this transport-level test doesn't need.
    @server.subscribe_to_services_once
    @server_nats.flush(5)
  end

  after(:all) do
    @server.subscription_manager.unsubscribe_all
    @server.subscription_manager.shutdown(2)
    @server.thread_pool.shutdown
    @server.thread_pool.wait_for_termination(5)
    @server_nats.close rescue nil

    ::Protobuf::Nats.config.servers = nil
    ::Protobuf::Nats.config.connection_options(true)
  end

  after do
    # Return the shared client connection + muxer singleton to a clean slate so
    # the (fake-connection) unit examples that run in the same process are
    # unaffected.
    connection = ::Protobuf::Nats.client_nats_connection
    ::Protobuf::Nats::Client::RESPONSE_MUXER.stop
    ::Protobuf::Nats.instance_variable_set(:@client_nats_connection, nil)
    connection.close rescue nil
  end

  it "completes a full RPC round trip (request -> ACK -> handler -> response)" do
    expect(rpc(new_client, "hello-integration")).to eq("hello-integration")
  end

  it "handles concurrent requests" do
    payloads = 10.times.map { |i| "concurrent-#{i}" }
    results = payloads.map { |p| ::Thread.new { rpc(new_client, p) } }.map(&:value)
    expect(results).to match_array(payloads)
  end

  it "self-heals after a terminal connection close (the muxer restarts on the new connection)" do
    expect(rpc(new_client, "before-close")).to eq("before-close")

    # Simulate nats-pure giving up: closing fires on_close, which drops the
    # memoized connection so the next request rebuilds it.
    old_connection = ::Protobuf::Nats.client_nats_connection
    old_connection.close
    wait_until(timeout: 5) { ::Protobuf::Nats.instance_variable_get(:@client_nats_connection).nil? }

    # The next client must build a fresh connection AND move the muxer's inbox
    # subscription onto it -- previously the muxer stayed subscribed to the
    # dead connection and every response was lost forever.
    client = new_client
    new_connection = ::Protobuf::Nats.client_nats_connection
    expect(new_connection).not_to equal(old_connection)
    expect(::Protobuf::Nats::Client::RESPONSE_MUXER.subscribed_to?(new_connection)).to be(true)

    expect(rpc(client, "after-close")).to eq("after-close")
  end
end
