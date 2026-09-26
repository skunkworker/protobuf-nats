require "erb"
require "openssl"
require "socket"
require "yaml"

module Protobuf
  module Nats
    class Config
      attr_accessor :uses_tls, :servers, :connect_timeout, :tls_client_cert, :tls_client_key, :tls_ca_cert, :max_reconnect_attempts, :connection_name
      attr_accessor :reconnect_time_wait, :ping_interval, :max_outstanding_pings
      attr_accessor :server_subscription_key_do_not_subscribe_to_when_includes_any_of,
                    :server_subscription_key_only_subscribe_to_when_includes_any_of,
                    :subscription_key_replacements

      CONFIG_MUTEX = ::Mutex.new

      DEFAULTS = {
        :connect_timeout => nil,
        # Per-server reconnect cap. -1 means forever (nats-pure treats
        # negative as infinite). Once exhausted on every server, nats-pure
        # fires on_close and the connection is dead for good.
        :max_reconnect_attempts => 60_000,
        # Failover tuning; nil uses the nats-pure defaults
        # (reconnect_time_wait: 2s, ping_interval: 120s,
        # max_outstanding_pings: 2). A silent node death is detected only
        # after ping_interval times max_outstanding_pings. Lower these for
        # faster failover.
        :reconnect_time_wait => nil,
        :ping_interval => nil,
        :max_outstanding_pings => nil,
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
              # safe_load blocks object deserialization; aliases stay on for
              # `&defaults` / `<<: *defaults`.
              parsed = ::YAML.safe_load(yaml_string, :aliases => true)

              # Guard nil: an empty file, or one missing the env section.
              yaml_config = (parsed && parsed[env]) || {}
            end

            # Apply false too: `if setting` skipped an explicit `false`
            # (e.g. `uses_tls: false`). A blank value (nil) keeps the
            # default, so a list option never becomes nil.
            DEFAULTS.each_key do |key|
              setting = yaml_config[key.to_s]
              __send__("#{key}=", setting) unless setting.nil?
            end
            warn_about_unknown_keys(yaml_config, absolute_config_path)

            # Reload the connection options hash
            connection_options(true)

            true
          end
        end
      end

      # Warn about a key the gem does not read. Fleet files set `hosts` and
      # `use_tls`, but the gem reads `servers` and `uses_tls`: a person who
      # sets `use_tls: true` gets no TLS, and before this, no warning.
      def warn_about_unknown_keys(yaml_config, path)
        return unless yaml_config.is_a?(::Hash)
        unknown = yaml_config.keys.map(&:to_s) - DEFAULTS.keys.map(&:to_s)
        return if unknown.empty?
        ::Protobuf::Logging.logger.warn "Ignoring unknown protobuf-nats config key(s) in #{path}: #{unknown.sort.join(", ")}. Known keys: #{DEFAULTS.keys.sort.join(", ")}"
      rescue ::StandardError
        nil
      end

      # Only the keys nats-pure's `connect` consumes. App-level settings
      # (uses_tls, tls_client_cert/key/ca_cert, server_subscription_key_*,
      # subscription_key_replacements) are read via their own accessors.
      # Do NOT forward them; nats-pure ignores unknown keys today, but that
      # is brittle. #new_tls_context folds the TLS settings into :tls.
      def connection_options(reload = false)
        @connection_options = false if reload
        @connection_options ||= begin
          options = {
            servers: servers,
            max_reconnect_attempts: max_reconnect_attempts,
            connect_timeout: connect_timeout,
            # nil is safe here; nats-pure fills each with its own default.
            reconnect_time_wait: reconnect_time_wait,
            ping_interval: ping_interval,
            max_outstanding_pings: max_outstanding_pings,
            # A friendly name helps NATS server monitoring, error reports,
            # and debugging. Both client and server build from this hash.
            name: resolved_connection_name,
          }
          options[:tls] = {:context => new_tls_context} if uses_tls
          options
        end
      end

      # Precedence: PB_NATS_CONNECTION_NAME env var, then config
      # connection_name, then hostname (never blank).
      def resolved_connection_name
        ::ENV["PB_NATS_CONNECTION_NAME"] || connection_name || ::Socket.gethostname
      end

      def new_tls_context
        tls_context = ::OpenSSL::SSL::SSLContext.new
        # Floor TLS 1.2, ceiling TLS 1.3 (replaces the deprecated
        # ssl_version=:TLSv1_2 pin). Negotiates the highest version the
        # server supports; a TLS-1.2-only server still connects (verified
        # on JRuby 9.4 and 10.0).
        #
        # An OpenSSL build without TLS 1.3 lacks TLS1_3_VERSION (#7);
        # degrade to 1.2-only instead of raising NameError.
        tls_context.min_version = ::OpenSSL::SSL::TLS1_2_VERSION
        tls_context.max_version = if defined?(::OpenSSL::SSL::TLS1_3_VERSION)
          ::OpenSSL::SSL::TLS1_3_VERSION
        else
          ::OpenSSL::SSL::TLS1_2_VERSION
        end
        tls_context.cert = ::OpenSSL::X509::Certificate.new(::File.read(tls_client_cert)) if tls_client_cert
        # PKey.read accepts any key type; PKey::RSA.new rejects non-RSA keys.
        tls_context.key = ::OpenSSL::PKey.read(::File.read(tls_client_key)) if tls_client_key

        # Verify the server's certificate chain. nats-pure uses a supplied
        # :tls context verbatim and does NOT call #set_params, so
        # verification must be set here. Without it, OpenSSL's VERIFY_NONE
        # default let any certificate through, including an attacker's.
        tls_context.verify_mode = ::OpenSSL::SSL::VERIFY_PEER
        cert_store = ::OpenSSL::X509::Store.new
        if tls_ca_cert
          # Trust the configured CA bundle (private-CA deployment).
          cert_store.add_file(tls_ca_cert)
        else
          # No CA configured: use the system trust store.
          cert_store.set_default_paths
        end
        tls_context.cert_store = cert_store

        # NOTE: hostname (SAN/CN) verification is still OFF. nats-pure only
        # sets the SSLSocket hostname when it builds the context itself; a
        # supplied context gets no hostname, and one static value would be
        # wrong for a multi-server cluster anyway. Chain verification above
        # still confirms the cert is CA-signed. Per-connection hostname
        # verification is separate future work.
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
