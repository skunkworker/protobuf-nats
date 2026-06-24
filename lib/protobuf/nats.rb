require "protobuf/nats/version"

require "protobuf"
# We don't need this, but the CLI attempts to terminate.
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

    # Eagerly load the yml config.
    config

    # We will always log  an error.
    def self.error_callbacks
      @error_callbacks ||= [lambda { |error| log_error(error) }]
    end

    # Eagerly load the yml config.
    error_callbacks

    def self.on_error(&block)
      fail ::ArgumentError unless block.arity == 1
      error_callbacks << block
      nil
    end

    # Single instrumentation entry point. Appends the gem's `.protobuf-nats`
    # suffix so callers don't repeat it (and can't typo it). Supports both the
    # value form `instrument("server.x", 5)` and the block form
    # `instrument("client.request_duration") { ... }`.
    def self.instrument(event, payload = {}, &block)
      ::ActiveSupport::Notifications.instrument("#{event}.protobuf-nats", payload, &block)
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

    # Bounded, single-thread executor for running error callbacks OFF hot/shared
    # threads (notably nats-pure's read/flush thread via on_error). A slow user
    # callback must not stall message processing for every subject. The queue is
    # bounded and over-capacity notifications are discarded (they're advisory).
    ERROR_CALLBACK_EXECUTOR = ::Concurrent::ThreadPoolExecutor.new(
      :min_threads => 0,
      :max_threads => 1,
      :max_queue => 1024,
      :fallback_policy => :discard
    )

    # Count of error callbacks discarded because the bounded executor was
    # saturated. Lets a flood of dropped callbacks during an incident be observed
    # instead of vanishing silently.
    ERROR_CALLBACK_DROP_COUNT = ::Concurrent::AtomicFixnum.new(0)

    def self.error_callback_drop_count
      ERROR_CALLBACK_DROP_COUNT.value
    end

    def self.notify_error_callbacks_async(error)
      # #post returns false when the job is rejected. With the :discard fallback
      # policy the job is silently dropped (returning false) rather than raising,
      # so the false return is the only drop signal to handle.
      accepted = ERROR_CALLBACK_EXECUTOR.post { notify_error_callbacks(error) }
      record_dropped_error_callback unless accepted
      nil
    end

    # Record a discarded error callback. Kept cheap -- this runs on nats-pure's
    # read/flush thread, so it must NOT format/log the error synchronously (the
    # whole point of the async path). The atomic counter is the durable signal;
    # the instrument gauge emits a discrete event for dashboards (drops only
    # happen under a severe flood, so a notification per drop is acceptable).
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

        # NOTE: nats-pure has no :disable_reconnect_buffer option (it was a
        # jnats concept). During a reconnect nats-pure buffers publishes and,
        # if the connection is fully closed, raises ConnectionClosedError --
        # both of which the client's transient-error retry path now handles.
        options = config.connection_options

        client = NatsClient.new

        # Register lifecycle callbacks BEFORE connecting so a disconnect or
        # error during the initial handshake is still observed.
        client.on_disconnect do
          logger.warn("Client NATS connection was disconnected")
        end

        client.on_reconnect do
          logger.warn("Client NATS connection was reconnected")
        end

        client.on_close do
          logger.warn("Client NATS connection was closed")
          # A close is terminal for this client object (nats-pure only reconnects
          # via on_disconnect/on_reconnect; on_close means it gave up). Drop the
          # memoized reference so the next start_client_nats_connection rebuilds a
          # fresh connection instead of reusing a permanently-dead one. In-flight
          # callers keep their own local reference; only new calls rebuild.
          @client_nats_connection = nil
        end

        client.on_error do |error|
          # Runs on nats-pure's read/flush thread -- offload so a slow callback
          # can't stall message processing.
          notify_error_callbacks_async(error)
        end

        begin
          client.connect(options)
          # Ensure we have a valid connection to the NATS server.
          client.flush(5)
        rescue => e
          # A failed handshake can leave nats-pure's reader/flusher threads
          # running on a half-open client; close it so we don't leak them, then
          # surface the failure (the next call will retry with a fresh client).
          client.close rescue nil
          raise e
        end

        @client_nats_connection = client

        true
      end
    end

    # Monotonic clock for durations/ages; immune to wall-clock (NTP) jumps.
    # Single source of truth shared by the client muxer and server pools.
    def self.monotonic_time
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end

    # Exponential backoff (seconds) for self-healing worker threads after a fatal
    # crash, capped. Shared by the ResponseMuxer dispatcher pool and the server
    # SuperSubscriptionManager handler pool so the formula can't drift between them.
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
