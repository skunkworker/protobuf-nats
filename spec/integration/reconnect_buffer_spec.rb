require "spec_helper"

# A NATS outage against a real single-node server (gated on the nats-server
# binary; see spec_helper). nats-pure buffers publishes while it reconnects
# and sends the buffer after the reconnect, so the server ran every retry
# the caller already saw fail. The muxer must refuse to publish instead.
describe "requests during a NATS reconnect", :integration_cluster => true do
  RECONNECT_SPEC_PORT = 14_224

  def port_open?
    ::Socket.tcp("127.0.0.1", RECONNECT_SPEC_PORT, :connect_timeout => 0.2).close
    true
  rescue ::StandardError
    false
  end

  def spawn_node
    pid = ::Process.spawn("nats-server", "-a", "127.0.0.1", "-p", RECONNECT_SPEC_PORT.to_s, :out => ::File::NULL, :err => ::File::NULL)
    wait_until(timeout: 10) { port_open? }
    pid
  end

  def kill_node(pid)
    ::Process.kill("KILL", pid)
    ::Process.wait(pid)
  rescue ::Errno::ESRCH, ::Errno::ECHILD
    # already gone
  end

  before do
    @pid = spawn_node
    ::Protobuf::Nats.config.servers = ["nats://127.0.0.1:#{RECONNECT_SPEC_PORT}"]
    ::Protobuf::Nats.config.reconnect_time_wait = 0.25
    ::Protobuf::Nats.config.connection_options(true)
    ::Protobuf::Nats.start_client_nats_connection
    ::Protobuf::Nats::Client::RESPONSE_MUXER.start
  end

  after do
    connection = ::Protobuf::Nats.client_nats_connection
    ::Protobuf::Nats::Client::RESPONSE_MUXER.stop
    ::Protobuf::Nats.instance_variable_set(:@client_nats_connection, nil)
    connection.close rescue nil
    @server_side.close rescue nil
    kill_node(@pid) if @pid

    ::Protobuf::Nats.config.servers = nil
    ::Protobuf::Nats.config.reconnect_time_wait = nil
    ::Protobuf::Nats.config.connection_options(true)
  end

  it "raises the retryable error while reconnecting, and replays nothing after the reconnect" do
    connection = ::Protobuf::Nats.client_nats_connection
    subject_name = "rpc.reconnect_buffer_spec.create"

    kill_node(@pid)
    wait_until(timeout: 10) { !connection.connected? }

    # Three attempts, as the client retry loop makes during an outage.
    3.times do |attempt|
      req = ::Protobuf::Nats::Client::RESPONSE_MUXER.new_request
      expect { req.publish(subject_name, "attempt-#{attempt}") }
        .to raise_error(::Protobuf::Nats::Errors::ResponseMuxer, /not connected/)
      req.cleanup
    end

    # NATS comes back. A subscriber that sees a replayed attempt fails this.
    @pid = spawn_node
    received = ::Queue.new
    @server_side = ::NATS::IO::Client.new
    @server_side.connect(:servers => ["nats://127.0.0.1:#{RECONNECT_SPEC_PORT}"])
    @server_side.subscribe(subject_name) { |msg| received << msg.data }
    @server_side.flush(2)

    wait_until(timeout: 10) { connection.connected? }
    connection.flush(2)
    @server_side.flush(2)

    expect(received.size).to eq(0)
  end
end
