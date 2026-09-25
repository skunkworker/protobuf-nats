require "protobuf/nats/version"

require "protobuf"
# Unused here, but the protobuf CLI calls ServiceDirectory#stop on shutdown.
require "protobuf/rpc/service_directory"

require "nats/io/client"
require "concurrent"



require "protobuf/nats/client"
require "protobuf/nats/config"
require "protobuf/nats/errors"
require "protobuf/nats/runner"
require "protobuf/nats/server"

require "protobuf/nats/response_muxer"
require "protobuf/nats/response_muxer_request"
require "protobuf/nats/super_subscription_manager"

module Protobuf
  module Nats
    class << self
      attr_accessor :client_nats_connection
    end

    module Messages
      ACK  = "\1".freeze
      NACK = "\2".freeze
    end

    NatsClient = ::NATS::IO::Client

    GET_CONNECTED_MUTEX = ::Mutex.new

    def self.config
      @config ||= begin
        config = ::Protobuf::Nats::Config.new
        config.load_from_yml
        config
      end
    end

    # Load the YAML config now.
    config

    # Always log an error.
    def self.error_callbacks
      @error_callbacks ||= [lambda { |error| log_error(error) }]
    end

    # Load the default error callback now.
    error_callbacks

    def self.on_error(&block)
      fail ::ArgumentError unless block.arity == 1
      error_callbacks << block
      nil
    end

    # Single entry point for instrumentation. Adds the `.protobuf-nats`
    # suffix so callers do not repeat it.
    #
    # ActiveSupport re-raises a subscriber's exception to this caller. The
    # gem instruments inside the request path, so one failing subscriber
    # (a closed statsd socket, an APM bug) dropped a response after its
    # ACK, or ended Server#run with no drain. Log and discard subscriber
    # errors here. An error from the block itself (the instrumented work)
    # still raises: the caller must see it.
    def self.instrument(event, payload = {})
      name = "#{event}.protobuf-nats"
      unless block_given?
        begin
          ::ActiveSupport::Notifications.instrument(name, payload)
        rescue ::StandardError => error
          record_subscriber_error(name, error)
        end
        return nil
      end

      block_ran = false
      block_error = nil
      result = nil
      begin
        ::ActiveSupport::Notifications.instrument(name, payload) do |event_payload|
          block_ran = true
          begin
            result = yield event_payload
          rescue ::Exception => error
            block_error = error
            raise
          end
        end
      rescue ::StandardError => error
        record_subscriber_error(name, error) unless error.equal?(block_error)
        # A subscriber's #finish can raise after the block raised, and its
        # error then replaces the block's error. The block's error wins.
        raise block_error if block_error
        # A subscriber's #start raised before the block ran. Do the work.
        result = yield payload unless block_ran
      end
      result
    end

    # Counts subscriber errors, like ERROR_CALLBACK_DROP_COUNT.
    SUBSCRIBER_ERROR_COUNT = ::Concurrent::AtomicFixnum.new(0)

    # Log the first subscriber error, then one in every 1,000. A broken
    # subscriber raises on every event; a full log line each time would
    # flood the log at the request rate.
    SUBSCRIBER_ERROR_LOG_EVERY = 1_000

    def self.subscriber_error_count
      SUBSCRIBER_ERROR_COUNT.value
    end

    # Do not instrument here: the failing subscriber could raise again.
    def self.record_subscriber_error(name, error)
      count = SUBSCRIBER_ERROR_COUNT.increment
      return unless count == 1 || (count % SUBSCRIBER_ERROR_LOG_EVERY).zero?
      logger.error "Ignored an error from an ActiveSupport::Notifications subscriber for #{name} (#{count} so far): #{error.class}: #{error.message}"
    rescue ::StandardError
      nil
    end

    def self.notify_error_callbacks(error)
      error_callbacks.each do |callback|
        begin
          callback.call(error)
        rescue => callback_error
          log_error(callback_error)
        end
      end

      nil
    end

    # Runs error callbacks off nats-pure's read/flush thread, so a slow
    # callback cannot stall message processing. Drops callbacks over the
    # queue limit; they are advisory only.
    ERROR_CALLBACK_EXECUTOR = ::Concurrent::ThreadPoolExecutor.new(
      :min_threads => 0,
      :max_threads => 1,
      :max_queue => 1024,
      :fallback_policy => :discard
    )

    # Counts error callbacks dropped when the queue is full. Makes a flood
    # of drops visible during an incident.
    ERROR_CALLBACK_DROP_COUNT = ::Concurrent::AtomicFixnum.new(0)

    def self.error_callback_drop_count
      ERROR_CALLBACK_DROP_COUNT.value
    end

    def self.notify_error_callbacks_async(error)
      # #post returns false on rejection. The :discard policy drops the
      # job instead of raising, so false is the only drop signal.
      accepted = ERROR_CALLBACK_EXECUTOR.post { notify_error_callbacks(error) }
      record_dropped_error_callback unless accepted
      nil
    end

    # Records a dropped callback. Runs on nats-pure's read/flush thread, so
    # do NOT format or log the error here; that would defeat the async path.
    def self.record_dropped_error_callback
      ERROR_CALLBACK_DROP_COUNT.increment
      instrument("error_callback_dropped", 1)
      nil
    end

    def self.subscription_key(service_klass, service_method)
      service_class_name = service_klass.name.underscore.gsub("/", ".")
      service_method_name = service_method.to_s.underscore

      subscription_key = "rpc.#{service_class_name}.#{service_method_name}"
      subscription_key = config.make_subscription_key_replacements(subscription_key)
    end

    def self.start_client_nats_connection
      return true if @client_nats_connection

      GET_CONNECTED_MUTEX.synchronize do
        break true if @client_nats_connection

        # nats-pure has no :disable_reconnect_buffer option (a jnats
        # concept). It buffers publishes during reconnect, and raises
        # ConnectionClosedError if the connection fully closes. The
        # client's retry path handles both cases.
        options = config.connection_options

        client = NatsClient.new

        # Register lifecycle callbacks before connecting, so the handshake
        # is also observed.
        client.on_disconnect do
          logger.warn("Client NATS connection was disconnected")
        end

        client.on_reconnect do
          logger.warn("Client NATS connection was reconnected")
        end

        client.on_close do
          logger.warn("Client NATS connection was closed")
          # A close is terminal (nats-pure only reconnects via
          # on_disconnect/on_reconnect). Clear the memo, so the next call
          # builds a fresh connection. Callers with a local reference keep it.
          @client_nats_connection = nil
        end

        client.on_error do |error|
          # Runs on nats-pure's read/flush thread; offload it so a slow
          # callback cannot stall message processing.
          notify_error_callbacks_async(error)
        end

        begin
          client.connect(options)
          # Confirm the connection is valid.
          client.flush(5)
        rescue => e
          # A failed handshake can leave nats-pure's threads running on a
          # half-open client. Close it to avoid a leak, then raise; the
          # next call retries with a fresh client.
          client.close rescue nil
          raise e
        end

        @client_nats_connection = client

        true
      end
    end

    # Monotonic clock for durations and ages, immune to wall-clock (NTP)
    # jumps. Shared by the client muxer and server pools.
    def self.monotonic_time
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end

    # Strict integer parsing for env overrides. String#to_i silently turns
    # a bad value ("5s", "abc") into 0, which fails every request instantly
    # for a timeout. Log an error and use the default instead. Also
    # rejects a value below `min`.
    def self.env_int(name, default, min: nil)
      raw = ::ENV[name]
      return default if raw.nil?

      value = Integer(raw, 10)
      if min && value < min
        logger.error "Ignoring out-of-range ENV #{name}=#{raw.inspect} (minimum #{min}); using default #{default}"
        return default
      end
      value
    rescue ::ArgumentError, ::TypeError
      logger.error "Ignoring malformed integer in ENV #{name}=#{raw.inspect}; using default #{default}"
      default
    end

    # Float version of env_int. Same strict-parse-or-default rule.
    def self.env_float(name, default)
      raw = ::ENV[name]
      return default if raw.nil?
      Float(raw)
    rescue ::ArgumentError, ::TypeError
      logger.error "Ignoring malformed number in ENV #{name}=#{raw.inspect}; using default #{default}"
      default
    end

    # Client response timeout, in seconds. The muxer sets its token TTL
    # longer than this (ResponseMuxer#token_ttl_seconds).
    def self.client_response_timeout
      env_int("PB_NATS_CLIENT_RESPONSE_TIMEOUT", 60)
    end

    # How long a consumer loop waits after its queue pops nil (a closed
    # queue always returns nil). Shared by the muxer and server intake
    # loops, so they cannot drift apart.
    CLOSED_QUEUE_PARK_SECONDS = 0.05

    # nats-pure only decrements pending_size (bytes) in its own consumption
    # paths. Server intake pops pending_queue directly and skips those
    # paths, so pending_size only grows and would eventually trip the byte
    # limit on cumulative traffic, dropping every later message. Disable
    # the byte limit; the message-count limit (pending_queue depth) still
    # catches a slow consumer. No-op on a non-standard subscription.
    #
    # NOTE: only the server uses this. The client muxer decrements
    # pending_size itself after each pop (ResponseMuxer#run_dispatch_loop)
    # and enforces its own byte ceiling.
    def self.disable_subscription_byte_limit!(sub)
      sub.pending_bytes_limit = ::Float::INFINITY if sub.respond_to?(:pending_bytes_limit=)
    end

    # Exponential backoff, in seconds, for a worker thread after a fatal
    # crash. Shared by the ResponseMuxer and SuperSubscriptionManager pools.
    def self.crash_backoff_seconds(crash_count, cap = 60)
      [(crash_count**2), cap].min
    end

    def self.log_error(error)
      logger.error error.to_s
      logger.error error.class.to_s
      if error.respond_to?(:backtrace) && error.backtrace.is_a?(::Array)
        logger.error error.backtrace.join("\n")
      end
    end

    def self.logger
      ::Protobuf::Logging.logger
    end

    logger.info "Using #{NatsClient} to connect"

    at_exit do
      ::Protobuf::Nats.client_nats_connection.close rescue nil
    end

  end
end
