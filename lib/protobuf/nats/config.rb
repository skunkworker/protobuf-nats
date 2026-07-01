require "erb"
require "openssl"
require "socket"
require "yaml"

module Protobuf
  module Nats
    class Config
      attr_accessor :uses_tls, :servers, :connect_timeout, :tls_client_cert, :tls_client_key, :tls_ca_cert, :max_reconnect_attempts, :connection_name
      attr_accessor :server_subscription_key_do_not_subscribe_to_when_includes_any_of,
                    :server_subscription_key_only_subscribe_to_when_includes_any_of,
                    :subscription_key_replacements

      CONFIG_MUTEX = ::Mutex.new

      DEFAULTS = {
        :connect_timeout => nil,
        :max_reconnect_attempts => 60_000,
        :servers => nil,
        :connection_name => nil,
        :tls_client_cert => nil,
        :tls_client_key => nil,
        :tls_ca_cert => nil,
        :uses_tls => false,
        :server_subscription_key_do_not_subscribe_to_when_includes_any_of => [],
        :server_subscription_key_only_subscribe_to_when_includes_any_of => [],
        :subscription_key_replacements => [],
      }.freeze

      def initialize
        DEFAULTS.each_pair do |key, value|
          __send__("#{key}=", value)
        end
      end

      def load_from_yml(reload = false)
        CONFIG_MUTEX.synchronize do
          @load_from_yml = nil if reload
          @load_from_yml ||= begin
            env = ENV["RAILS_ENV"] || ENV["RACK_ENV"] || ENV["APP_ENV"] || "development"

            yaml_config = {}
            config_path = ENV["PROTOBUF_NATS_CONFIG_PATH"] || ::File.join("config", "protobuf_nats.yml")
            absolute_config_path = ::File.expand_path(config_path)
            if ::File.exist?(absolute_config_path)
              yaml_string = ::ERB.new(::File.read(absolute_config_path)).result
              # safe_load (no arbitrary object deserialization) with aliases
              # enabled so the common `&defaults` / `<<: *defaults` pattern works.
              parsed = ::YAML.safe_load(yaml_string, :aliases => true)

              # An empty file parses to nil/false, and a file without a section
              # for the current env yields nil on lookup -- guard both so we
              # don't blow up with NoMethodError below.
              yaml_config = (parsed && parsed[env]) || {}
            end

            DEFAULTS.each_pair do |key, value|
              setting = yaml_config[key.to_s]
              __send__("#{key}=", setting) if setting
            end

            # Reload the connection options hash
            connection_options(true)

            true
          end
        end
      end

      # Only the keys nats-pure's `connect` actually consumes. App-level settings
      # (uses_tls, tls_client_cert, tls_client_key, tls_ca_cert,
      # server_subscription_key_*, subscription_key_replacements) are read
      # directly via their accessors elsewhere and must NOT be forwarded to
      # nats-pure (it ignores unknown keys today, but that is brittle). The TLS
      # cert/key/CA are folded into the :tls context by #new_tls_context.
      def connection_options(reload = false)
        @connection_options = false if reload
        @connection_options ||= begin
          options = {
            servers: servers,
            max_reconnect_attempts: max_reconnect_attempts,
            connect_timeout: connect_timeout,
            # A friendly connection name surfaces in NATS server monitoring,
            # error reporting, and debugging (highly recommended by the NATS
            # docs). Shared by both the client and server connections since both
            # build from this hash.
            name: resolved_connection_name,
          }
          options[:tls] = {:context => new_tls_context} if uses_tls
          options
        end
      end

      # Precedence: PB_NATS_CONNECTION_NAME env var > yaml/DEFAULT connection_name
      # > hostname. Env wins so ops can set a per-pod/per-host name without a
      # config file; the hostname fallback ensures the name is never blank.
      def resolved_connection_name
        ::ENV["PB_NATS_CONNECTION_NAME"] || connection_name || ::Socket.gethostname
      end

      def new_tls_context
        tls_context = ::OpenSSL::SSL::SSLContext.new
        # Floor at TLS 1.2, ceiling at TLS 1.3 (replaces the deprecated
        # ssl_version=:TLSv1_2 hard pin). The client offers 1.2 and 1.3 and
        # negotiates the highest the server also supports, so a TLS-1.2-only
        # transport still connects (verified on JRuby 9.4 and 10.0).
        #
        # NOTE (#7): this assumes the OpenSSL build defines TLS1_3_VERSION. That
        # holds on the JRuby targets above, but an older MRI/OpenSSL build without
        # the constant would raise NameError here. Not guarded yet -- revisit if
        # CRuby-on-old-OpenSSL becomes a supported target.
        tls_context.min_version = ::OpenSSL::SSL::TLS1_2_VERSION
        tls_context.max_version = ::OpenSSL::SSL::TLS1_3_VERSION
        tls_context.cert = ::OpenSSL::X509::Certificate.new(::File.read(tls_client_cert)) if tls_client_cert
        tls_context.key = ::OpenSSL::PKey::RSA.new(::File.read(tls_client_key)) if tls_client_key

        # Verify the NATS server's certificate chain. This context is handed to
        # nats-pure as :tls => {:context => ...}; nats-pure uses a supplied
        # context verbatim and does NOT call #set_params, so verification has to
        # be configured here. Without this the OpenSSL default (VERIFY_NONE)
        # stood and any certificate -- including an attacker's -- was accepted.
        tls_context.verify_mode = ::OpenSSL::SSL::VERIFY_PEER
        cert_store = ::OpenSSL::X509::Store.new
        if tls_ca_cert
          # Trust the configured CA bundle (the private-CA deployment case).
          cert_store.add_file(tls_ca_cert)
        else
          # No CA configured: fall back to the system trust store.
          cert_store.set_default_paths
        end
        tls_context.cert_store = cert_store

        # NOTE: hostname (SAN/CN) verification is NOT enabled here. nats-pure only
        # sets the SSLSocket hostname from @tls[:hostname], which it populates
        # itself only when it builds the context; for a supplied context it stays
        # nil, and a single static hostname would be wrong for a multi-server
        # cluster that reconnects across hosts. Chain verification above still
        # ensures the cert is signed by the trusted CA. Plumbing per-connection
        # hostname verification is tracked separately.
        tls_context
      end

      def make_subscription_key_replacements(subscription_key)
        subscription_key_replacements.each do |replacement|
          match = replacement.keys.first
          replacement = replacement[match]

          if subscription_key.include?(match)
            return subscription_key.gsub(match, replacement)
          end
        end

        subscription_key
      end
    end
  end
end
