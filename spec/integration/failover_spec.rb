require "spec_helper"

# Failover across a real two-node NATS cluster (gated on the nats-server
# binary; see spec_helper). This is the one test that exercises the behaviors
# the gem relies on from nats-pure internals -- server-pool failover,
# subscription replay on reconnect -- against a real cluster, so a nats-pure
# upgrade that changes them fails here instead of in production.
class FailoverPing < ::Protobuf::Message
  optional :string, :payload, 1
end

class FailoverEchoService < ::Protobuf::Rpc::Service
  rpc :echo, FailoverPing, FailoverPing

  def echo
    respond_with ::FailoverPing.new(:payload => request.payload)
  end
end

describe "failover across a two-node NATS cluster", :integration_cluster => true do
  NODE_PORTS = { 14_222 => 14_248, 14_223 => 14_249 }.freeze # client port => cluster port

  def port_open?(port)
    ::Socket.tcp("127.0.0.1", port, :connect_timeout => 0.2).close
    true
  rescue ::StandardError
    false
  end

  def spawn_node(client_port, cluster_port, route_port)
    pid = ::Process.spawn(
      "nats-server",
      "-a", "127.0.0.1",
      "-p", client_port.to_s,
      "--cluster_name", "pb-nats-failover",
      "--cluster", "nats://127.0.0.1:#{cluster_port}",
      "--routes", "nats://127.0.0.1:#{route_port}",
      :out => ::File::NULL, :err => ::File::NULL
    )
    wait_until(timeout: 10) { port_open?(client_port) }
    pid
  end

  def kill_node(pid)
    ::Process.kill("KILL", pid) # hard kill: no FIN handshake, like a dead host
    ::Process.wait(pid)
  rescue ::Errno::ESRCH, ::Errno::ECHILD
    # already gone
  end

  def build_request_data(payload)
    ::Protobuf::Socketrpc::Request.new(
      :service_name => "FailoverEchoService",
      :method_name => "echo",
      :request_proto => ::FailoverPing.new(:payload => payload).encode,
      :caller => "failover-spec"
    ).encode
  end

  def new_client
    ::Protobuf::Nats::Client.new(:service => FailoverEchoService, :method => :echo)
  end

  # Drives an RPC the way the production loop does: NACKs, ack timeouts, and
  # transient transport errors are all retried, because during the failover
  # window every one of those is expected.
  def rpc_with_retry(payload, attempts: 40)
    request_data = build_request_data(payload)
    opts = { :ack_timeout => 2, :timeout => 5 }
    last = nil
    attempts.times do
      client = new_client
      begin
        last = client.nats_request_with_two_responses(client.cached_subscription_key, request_data, opts)
      rescue *::Protobuf::Nats::Errors::RETRYABLE_TRANSPORT_ERRORS => e
        last = e
        sleep 0.25
        next
      end
      break unless last.is_a?(::Symbol) # :nack / :ack_timeout -> retry
      sleep 0.25
    end
    raise "request did not complete: #{last.inspect}" if last.is_a?(::Symbol) || last.is_a?(::Exception)

    response = ::Protobuf::Socketrpc::Response.decode(last)
    raise "rpc error: #{response.error}" unless response.error.to_s.empty?
    ::FailoverPing.decode(response.response_proto).payload
  end

  before(:all) do
    ports = NODE_PORTS.keys
    clusters = NODE_PORTS.values
    @node_pids = {
      ports[0] => spawn_node(ports[0], clusters[0], clusters[1]),
      ports[1] => spawn_node(ports[1], clusters[1], clusters[0]),
    }

    ::Protobuf::Nats.config.servers = ports.map { |p| "nats://127.0.0.1:#{p}" }
    # Fast failover for the test (also dogfoods the new config keys).
    ::Protobuf::Nats.config.reconnect_time_wait = 0.25
    ::Protobuf::Nats.config.connection_options(true)

    @server_nats = ::Protobuf::Nats::NatsClient.new
    @server = ::Protobuf::Nats::Server.new(:threads => 2, :client => @server_nats, :server => "failover-spec")
    @server.subscribe_to_services_once
    @server_nats.flush(5)
  end

  after(:all) do
    @server.subscription_manager.unsubscribe_all rescue nil
    @server.subscription_manager.shutdown(2) rescue nil
    @server.thread_pool.shutdown
    @server.thread_pool.wait_for_termination(5)
    @server_nats.close rescue nil

    ::Protobuf::Nats.config.servers = nil
    ::Protobuf::Nats.config.reconnect_time_wait = nil
    ::Protobuf::Nats.config.connection_options(true)

    (@node_pids || {}).each_value { |pid| kill_node(pid) }
  end

  after do
    # Same clean-slate reset as real_nats_spec: the shared connection + muxer
    # singleton must not leak into the fake-connection unit examples.
    connection = ::Protobuf::Nats.client_nats_connection
    ::Protobuf::Nats::Client::RESPONSE_MUXER.stop
    ::Protobuf::Nats.instance_variable_set(:@client_nats_connection, nil)
    connection.close rescue nil
  end

  it "keeps serving RPCs after the node the client is connected to dies" do
    expect(rpc_with_retry("before-failover")).to eq("before-failover")

    connection = ::Protobuf::Nats.client_nats_connection
    killed_port = connection.connected_server.port
    surviving_port = (NODE_PORTS.keys - [killed_port]).first

    kill_node(@node_pids.fetch(killed_port))
    wait_until(timeout: 10) { !port_open?(killed_port) }

    # The client connection must fail over to the surviving node (nats-pure
    # walks the server pool and replays the muxer's inbox subscription), and
    # the gem server -- whichever node it was on -- must keep answering.
    expect(rpc_with_retry("after-failover")).to eq("after-failover")
    expect(::Protobuf::Nats.client_nats_connection.connected_server.port).to eq(surviving_port)
  end
end
