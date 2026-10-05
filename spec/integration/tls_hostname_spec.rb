require "spec_helper"
require "tmpdir"

# TLS hostname verification against a real nats-server (gated on the
# nats-server binary; see spec_helper). The server cert names only
# "localhost", so a connect to 127.0.0.1 has a valid chain but the wrong
# host. JRuby ignores SSLContext#verify_hostname; this checks that
# TlsHostnameCheck enforces it on every Ruby.
describe "TLS hostname verification", :integration_cluster => true do
  TLS_PORT = 14_333

  def port_open?(port)
    ::TCPSocket.new("127.0.0.1", port, :connect_timeout => 0.2).close
    true
  rescue ::StandardError
    false
  end

  def build_cert(subject_cn, issuer_cert, issuer_key, key, ca:, extensions: [])
    cert = ::OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = rand(1 << 32)
    cert.subject = ::OpenSSL::X509::Name.parse("/CN=#{subject_cn}")
    cert.issuer = issuer_cert ? issuer_cert.subject : cert.subject
    cert.public_key = key.public_key
    cert.not_before = ::Time.now - 60
    cert.not_after = ::Time.now + 3600
    factory = ::OpenSSL::X509::ExtensionFactory.new
    factory.subject_certificate = cert
    factory.issuer_certificate = issuer_cert || cert
    cert.add_extension(factory.create_extension("basicConstraints", ca ? "CA:TRUE" : "CA:FALSE", true))
    extensions.each { |name, value| cert.add_extension(factory.create_extension(name, value)) }
    cert.sign(issuer_key, ::OpenSSL::Digest.new("SHA256"))
    cert
  end

  def connect(host, verify_hostname:)
    config = ::Protobuf::Nats::Config.new
    config.uses_tls = true
    config.tls_ca_cert = ::File.join(@dir, "ca.pem")
    config.tls_verify_hostname = verify_hostname
    client = ::NATS::IO::Client.new
    client.connect(:servers => ["nats://#{host}:#{TLS_PORT}"], :tls => { :context => config.new_tls_context },
                   :max_reconnect_attempts => 0, :connect_timeout => 3)
    client
  end

  before(:all) do
    @dir = ::Dir.mktmpdir("pb-nats-tls")
    ca_key = ::OpenSSL::PKey::RSA.new(2048)
    ca_cert = build_cert("pb-nats-test-ca", nil, ca_key, ca_key, :ca => true)
    server_key = ::OpenSSL::PKey::RSA.new(2048)
    server_cert = build_cert("localhost", ca_cert, ca_key, server_key, :ca => false,
                             :extensions => [["subjectAltName", "DNS:localhost"]])
    ::File.write(::File.join(@dir, "ca.pem"), ca_cert.to_pem)
    ::File.write(::File.join(@dir, "server.pem"), server_cert.to_pem)
    ::File.write(::File.join(@dir, "server-key.pem"), server_key.to_pem)

    # No "-a 127.0.0.1": "localhost" can resolve to ::1 first, and on
    # CRuby nats-pure then fails in setsockopt instead of trying 127.0.0.1.
    @pid = ::Process.spawn(
      "nats-server",
      "-p", TLS_PORT.to_s,
      "--tls",
      "--tlscert", ::File.join(@dir, "server.pem"),
      "--tlskey", ::File.join(@dir, "server-key.pem"),
      :out => ::File::NULL, :err => ::File::NULL
    )
    wait_until(timeout: 10) { port_open?(TLS_PORT) }
  end

  after(:all) do
    if @pid
      ::Process.kill("KILL", @pid) rescue nil
      ::Process.wait(@pid) rescue nil
    end
    ::FileUtils.remove_entry(@dir) if @dir
  end

  it "connects when the cert names the host" do
    client = connect("localhost", :verify_hostname => true)
    expect(client).to be_connected
  ensure
    client&.close
  end

  it "rejects a CA-signed cert for another host" do
    expect { connect("127.0.0.1", :verify_hostname => true).close }.to raise_error(::OpenSSL::SSL::SSLError)
  end

  it "accepts a CA-signed cert for another host when the check is off" do
    client = connect("127.0.0.1", :verify_hostname => false)
    expect(client).to be_connected
  ensure
    client&.close
  end
end
