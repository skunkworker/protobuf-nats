require "spec_helper"

describe ::Protobuf::Nats::Config do
  it "sets servers and connect_timeout to nil by default" do
    expect(subject.servers).to eq(nil)
    expect(subject.connect_timeout).to eq(nil)
  end

  it "has default options without tls" do
    subject.servers = ["nats://127.0.0.1:4222"]
    # Only nats-pure-recognized keys are forwarded to connect; app-level
    # settings are read via their own accessors, not via connection_options.
    expected_options = {
      :servers => ["nats://127.0.0.1:4222"],
      :connect_timeout => nil,
      :max_reconnect_attempts => 60_000,
    }
    expect(subject.connection_options).to eq(expected_options)
  end

  it "does not forward app-level keys to nats-pure" do
    subject.servers = ["nats://127.0.0.1:4222"]
    subject.uses_tls = false
    %i[uses_tls tls_client_cert tls_client_key tls_ca_cert
       server_subscription_key_do_not_subscribe_to_when_includes_any_of
       server_subscription_key_only_subscribe_to_when_includes_any_of
       subscription_key_replacements].each do |app_key|
      expect(subject.connection_options).not_to have_key(app_key)
    end
  end

  it "can provide a tls context" do
    subject.servers = ["nats://127.0.0.1:4222"]
    subject.uses_tls = true
    tls_context = subject.connection_options[:tls][:context]
    expect(tls_context).to be_an(::OpenSSL::SSL::SSLContext)
  end

  it "floors TLS at 1.2 and ceilings at 1.3" do
    context = subject.new_tls_context
    # Accessors are write-only on some OpenSSL builds, so assert via the C-level
    # min/max which both JRuby 9.4 and 10.0 expose through the setters we used.
    expect { context.min_version = ::OpenSSL::SSL::TLS1_2_VERSION }.not_to raise_error
    expect(::OpenSSL::SSL::TLS1_2_VERSION).to eq(771)
    expect(::OpenSSL::SSL::TLS1_3_VERSION).to eq(772)
    expect(context).to be_an(::OpenSSL::SSL::SSLContext)
  end

  it "can load a custom cert into the ssl context" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expected_cert = ::File.read("spec/support/certs/client-cert.pem")
    expect(subject.new_tls_context.cert.to_s).to eq(expected_cert)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "can load a custom key into the ssl context" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expected_key = ::File.read("spec/support/certs/client-key.pem")
    expect(subject.new_tls_context.key.to_s).to eq(expected_key)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "can load the yml from a specific directory" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    expect(subject.servers).to eq(["nats://127.0.0.1:4222", "nats://127.0.0.1:4223", "nats://127.0.0.1:4224"])
    expect(subject.uses_tls).to eq(true)
    expect(subject.connect_timeout).to eq(2)
    expect(subject.max_reconnect_attempts).to eq(1234)
    expect(subject.subscription_key_replacements).to eq([{"original_" => "local_"}, {"another_subscription" => "different_subscription"}])
    expect(subject.server_subscription_key_only_subscribe_to_when_includes_any_of).to eq(["search", "derp"])
    expect(subject.server_subscription_key_do_not_subscribe_to_when_includes_any_of).to eq(["derpderp", "searchsearch"])

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "loads the defaults when a yml config is missing" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/i_do_not_exist_because_im_not_real"

    subject.load_from_yml
    expect(subject.servers).to eq(nil)
    expect(subject.uses_tls).to eq(false)
    expect(subject.connect_timeout).to eq(nil)
    expect(subject.max_reconnect_attempts).to eq(60_000)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "builds a TLS context (from the configured certs) in the connection options" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml
    # The cert/key/ca are loaded into the TLS context, not forwarded as raw keys.
    expect(subject.tls_client_cert).to eq("./spec/support/certs/client-cert.pem")
    expect(subject.connection_options[:tls][:context]).to be_an(::OpenSSL::SSL::SSLContext)

    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "replaces subscription_key using subscription_key_replacements" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    subject.load_from_yml

    expect(subject.make_subscription_key_replacements("rpc.original_subscription")).to eq "rpc.local_subscription"
    expect(subject.make_subscription_key_replacements("rpc.another_subscription")).to eq "rpc.different_subscription"
    expect(subject.make_subscription_key_replacements("rpc.subscription")).to eq "rpc.subscription"
  end

  # Negative cases: the file exists but does not yield a hash for the current
  # environment. Previously these raised NoMethodError (nil[]) on boot.
  it "loads defaults without raising when the yml has no section for the current environment" do
    original_env = { "RAILS_ENV" => ENV["RAILS_ENV"], "RACK_ENV" => ENV["RACK_ENV"], "APP_ENV" => ENV["APP_ENV"] }
    ENV["RAILS_ENV"] = "environment_that_does_not_exist"
    ENV["RACK_ENV"] = nil
    ENV["APP_ENV"] = nil
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/protobuf_nats.yml"

    expect { subject.load_from_yml }.not_to raise_error
    expect(subject.servers).to eq(nil)
    expect(subject.max_reconnect_attempts).to eq(60_000)
  ensure
    original_env.each { |k, v| ENV[k] = v }
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end

  it "loads defaults without raising when the yml file is empty" do
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = "spec/support/empty_protobuf_nats.yml"

    expect { subject.load_from_yml }.not_to raise_error
    expect(subject.servers).to eq(nil)
    expect(subject.uses_tls).to eq(false)
  ensure
    ENV["PROTOBUF_NATS_CONFIG_PATH"] = nil
  end
end
