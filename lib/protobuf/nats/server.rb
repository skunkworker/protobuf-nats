require "active_support"
require "active_support/core_ext/class/subclasses"
require "concurrent"
require "protobuf/rpc/server"
require "protobuf/rpc/service"
require "protobuf/nats/thread_pool"
require "protobuf/nats/uuidv7_helper"

module Protobuf
  module Nats
    class Server
      include ::Protobuf::Rpc::Server
      include ::Protobuf::Logging

      attr_reader :nats, :thread_pool, :subscription_manager

      MILLISECOND = 1000

      def initialize(options)
        @options = options
        @processing_requests = true
        @running = true
        @stopped = false
        @pause_mutex = ::Mutex.new

        @nats = @options[:client] || ::Protobuf::Nats::NatsClient.new

        # Register callbacks before connect, to catch handshake errors too.
        @nats.on_disconnect do
          logger.warn "Server NATS connection was disconnected"
        end

        @nats.on_reconnect do
          logger.warn "Server NATS connection was reconnected"
        end

        @nats.on_error do |error|
          # This runs on the nats-pure read/flush thread. Go async so a
          # slow callback cannot block intake.
          ::Protobuf::Nats.notify_error_callbacks_async(error)
        end

        @nats.on_close do
          handle_connection_closed
        end

        # Bounded first connect: with NATS down, fail in seconds and let the
        # supervisor restart the process, not block boot for hours.
        ::Protobuf::Nats.initial_connect(@nats)

        @thread_pool = ::Protobuf::Nats::ThreadPool.new(threads, :max_queue => max_queue_size)

        @subscription_manager = ::Protobuf::Nats::SuperSubscriptionManager.new(@nats) do |request_data, reply_id, subject|
          # See #stale_request_ms for why we drop old requests here.
          next if stale_request?(reply_id)

          unless enqueue_request(request_data, reply_id)
            logger.error { "Thread pool is full! Dropping message for subject: #{subject}" }
          end
        end
        @server = options.fetch(:server, ::Socket.gethostname)

        # Track in-flight handlers for reporting only; never aborted.
        # @overdue_flagged stops us reporting the same one twice.
        @inflight = ::Concurrent::Map.new
        @overdue_flagged = ::Concurrent::Map.new
        @request_seq = ::Concurrent::AtomicFixnum.new(0)
      end

      def monotonic
        ::Protobuf::Nats.monotonic_time
      end

      def handler_count
        subscription_manager.handler_count
      end

      # Threshold for reporting a slow handler. Default 0 (off).
      def slow_handler_threshold_ms
        @slow_handler_threshold_ms ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_SLOW_HANDLER_THRESHOLD_MS", 0)
      end

      # Age (ms) at which we drop a request instead of running it. Past
      # this age the client has likely retried or given up, so the work
      # is wasted and can duplicate effects on non-idempotent RPCs.
      # Default 0 (off). Age comes from a UUIDv7 token in the reply inbox,
      # which encodes client wall-clock time: enable only with synced
      # clocks (NTP), and set well above the client's 5s ack_timeout.
      def stale_request_ms
        @stale_request_ms ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_STALE_REQUEST_MS", 0)
      end

      def stale_request?(reply_id)
        return false unless stale_request_ms.positive?

        age_ms = ::Protobuf::Nats::UUIDv7Helper.age_ms(reply_id.to_s[/[^.]*\z/])
        return false if age_ms.nil? || age_ms < stale_request_ms

        logger.debug { "Dropping stale request (age=#{age_ms}ms >= #{stale_request_ms}ms); the client has already retried or timed out" }
        ::Protobuf::Nats.instrument "server.stale_request_dropped", age_ms
        true
      end

      # Age (ms) at which a handler is "overdue": the client has already
      # given up (response_timeout), so it now holds a pool slot for
      # nothing. Default is above the client's 60s response_timeout, so
      # normal handlers are never flagged.
      def handler_overdue_ms
        @handler_overdue_ms ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_HANDLER_OVERDUE_MS", 65_000)
      end

      # Whether to abort an overdue handler to reclaim its pool slot. Off
      # by default: our contract is that handlers are never aborted, since
      # killing a thread mid-handler can corrupt state. Enable only if
      # overdue handlers are saturating the pool and healthy traffic gets
      # NACKed. Reclaim raises `Errors::HandlerOverdue` in the worker,
      # which the handler rescue turns into an RPC error response.
      def reclaim_overdue_handlers?
        # Memoize the raw string, not the boolean. A memoized `false` looks
        # unset to `||=` and would be recomputed every time.
        @reclaim_overdue_handlers ||= ::ENV.fetch("PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS", "false")
        @reclaim_overdue_handlers == "true"
      end

      # How long to wait for handlers to finish on shutdown. Tracks the
      # overdue window plus a grace period, so a long handler is not
      # killed mid-flight.
      def shutdown_drain_timeout
        @shutdown_drain_timeout ||= ::Protobuf::Nats.env_float("PB_NATS_SERVER_SHUTDOWN_DRAIN_TIMEOUT", (handler_overdue_ms / 1000.0) + 5)
      end

      def instrument_thread_pool_sizes
        ::Protobuf::Nats.instrument("server.thread_pool_enqueued_size", thread_pool.enqueued_size)
        ::Protobuf::Nats.instrument("server.thread_pool_max_size", thread_pool.max_size)
        ::Protobuf::Nats.instrument("server.thread_pool_running_size", thread_pool.size)
      end

      # Report in-flight handler health. `inflight_oldest_age_ms` can
      # normally approach response_timeout; only `overdue_handler_count`
      # signals a real problem.
      def instrument_inflight_handlers
        now = monotonic
        overdue_ms = handler_overdue_ms
        count = 0
        oldest_age_ms = 0.0
        overdue = 0

        @inflight.each_pair do |id, entry|
          started_at, handler_thread = entry
          count += 1
          age_ms = (now - started_at) * MILLISECOND
          oldest_age_ms = age_ms if age_ms > oldest_age_ms
          next unless overdue_ms.positive? && age_ms >= overdue_ms

          overdue += 1

          # Reclaim the slot by aborting the handler, if enabled (see
          # #reclaim_overdue_handlers?). The @inflight re-check narrows
          # the chance the raise lands on a worker already on a new
          # request; ThreadPool also swallows a raise between tasks.
          if reclaim_overdue_handlers? && handler_thread&.alive? && @inflight[id].equal?(entry)
            logger.warn "Reclaiming overdue handler (age=#{age_ms.round}ms, client already gave up) to free its pool slot"
            handler_thread.raise(::Protobuf::Nats::Errors::HandlerOverdue, "handler exceeded #{overdue_ms}ms; reclaimed")
            ::Protobuf::Nats.instrument("server.handler_reclaimed", age_ms)
          end

          # Report each overdue handler once; its result is discarded.
          next if @overdue_flagged[id]
          @overdue_flagged[id] = true
          logger.warn "Handler exceeded #{overdue_ms}ms (client already gave up); in-flight age=#{age_ms.round}ms"
          ::Protobuf::Nats.instrument("server.handler_overdue", age_ms)
        end

        # Live count, not the construction-time one: a handler killed by a
        # non-StandardError must show up here before #replenish restores it.
        ::Protobuf::Nats.instrument("server.subscription_handler_count", subscription_manager.live_handler_count)
        ::Protobuf::Nats.instrument("server.pending_intake_queue_size", subscription_manager.pending_queue_size)
        ::Protobuf::Nats.instrument("server.pending_intake_queue_bytes", subscription_manager.pending_queue_bytes)
        ::Protobuf::Nats.instrument("server.inflight_count", count)
        ::Protobuf::Nats.instrument("server.inflight_oldest_age_ms", oldest_age_ms)
        ::Protobuf::Nats.instrument("server.overdue_handler_count", overdue)

        # Remove orphaned overdue flags. A race is possible: we read id
        # from @inflight, the handler's `ensure` deletes both maps, then
        # we set @overdue_flagged[id] here. Nothing else removes that
        # entry, so clear any flag whose id is no longer in-flight.
        @overdue_flagged.each_key do |id|
          @overdue_flagged.delete(id) unless @inflight.key?(id)
        end
      end

      # Uses #threads, not the raw option, so a queue always matches the
      # actual worker count.
      def max_queue_size
        ::Protobuf::Nats.env_int("PB_NATS_SERVER_MAX_QUEUE_SIZE", threads)
      end

      def slow_start_delay
        @slow_start_delay ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_SLOW_START_DELAY", 10)
      end

      def subscriptions_per_rpc_endpoint
        @subscriptions_per_rpc_endpoint ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT", 10)
      end

      def threads
        @options[:threads] || 10
      end

      def service_klasses
        ::Protobuf::Rpc::Service.implemented_services.map(&:safe_constantize)
      end

      def enqueue_request(request_data, reply_id)
        ::Protobuf::Nats.instrument "server.message_received"

        enqueued_at = monotonic
        request_id = @request_seq.increment
        was_enqueued = thread_pool.push do
          # nil response_data means "handler failed, skip the success
          # publish". A successful encode is always a non-nil String.
          response_data = nil
          begin
            processed_at = monotonic
            ::Protobuf::Nats.instrument("server.thread_pool_execution_delay", (processed_at - enqueued_at) * MILLISECOND)

            # Track this handler as in-flight, for reporting only (see
            # #reclaim_overdue_handlers?).
            @inflight[request_id] = [processed_at, ::Thread.current]

            # Wrap only the handler here, so a publish failure below does
            # not land in this rescue and send a duplicate response.
            begin
              response_data = handle_request(request_data, 'server' => @server)
            rescue => error
              response_data = nil # ensure the success-publish below is skipped
              logger.debug { "rescued error => #{error}" }  if logger.debug?
              # Log the real error server-side; the client gets only a
              # generic message.
              ::Protobuf::Nats.notify_error_callbacks(error)

              # The client already got our ACK and now waits for a
              # response. Without one it hangs until response_timeout
              # (60s default). Send a generic RPC error instead, without
              # leaking error.message over the wire.
              begin
                error_response = ::Protobuf::Rpc::PbError.new("Internal server error")
                nats.publish(reply_id, error_response.encode)
              rescue => publish_error
                logger.error "Failed to publish error response for #{reply_id}: #{publish_error.message}"
              end
            end

            # Publish outside the handler rescue, so a failure here is
            # logged instead of sending a duplicate error response.
            if response_data
              logger.debug { "Publishing response to #{reply_id}" } if logger.debug?
              begin
                nats.publish(reply_id, response_data)
              rescue => publish_error
                logger.error "Failed to publish response for #{reply_id}: #{publish_error.message}"
                ::Protobuf::Nats.notify_error_callbacks(publish_error)
              end
            end
          ensure
            @inflight.delete(request_id)
            @overdue_flagged.delete(request_id)

            completed_at = monotonic
            ::Protobuf::Nats.instrument("server.request_duration", (completed_at - enqueued_at) * MILLISECOND)

            # Report a slow handler, if enabled (default off).
            if processed_at && slow_handler_threshold_ms.positive?
              handler_ms = (completed_at - processed_at) * MILLISECOND
              if handler_ms >= slow_handler_threshold_ms
                logger.warn "Slow handler for #{reply_id}: #{handler_ms.round}ms"
                ::Protobuf::Nats.instrument("server.slow_handler", handler_ms)
              end
            end
          end
        end

        # Send an ACK, or a NACK if the pool was full.
        begin
          if was_enqueued
            logger.debug { "[reply_id=#{reply_id}] Sending ACK" } if logger.debug?
            nats.publish(reply_id, ::Protobuf::Nats::Messages::ACK)
          else # Drop message if the thread pool is full
            ::Protobuf::Nats.instrument "server.thread_pool_saturated"
            ::Protobuf::Nats.instrument "server.message_dropped"
            logger.debug { "[reply_id=#{reply_id}] Sending NACK" } if logger.debug?
            nats.publish(reply_id, ::Protobuf::Nats::Messages::NACK)
          end
        rescue => e
          logger.error "Failed to send ACK/NACK for #{reply_id}: #{e.message}"
          ::Protobuf::Nats.notify_error_callbacks(e)
        end

        was_enqueued
      end

      def do_not_subscribe_to_includes?(subscription_key)
        return false unless ::Protobuf::Nats.config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.respond_to?(:any?)
        return false if ::Protobuf::Nats.config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.empty?

        ::Protobuf::Nats.config.server_subscription_key_do_not_subscribe_to_when_includes_any_of.any? do |key|
          subscription_key.include?(key)
        end
      end

      def only_subscribe_to_includes?(subscription_key)
        return true unless ::Protobuf::Nats.config.server_subscription_key_only_subscribe_to_when_includes_any_of.respond_to?(:any?)
        return true if ::Protobuf::Nats.config.server_subscription_key_only_subscribe_to_when_includes_any_of.empty?

        ::Protobuf::Nats.config.server_subscription_key_only_subscribe_to_when_includes_any_of.any? do |key|
          subscription_key.include?(key)
        end
      end

      def pause_file_path
        ::ENV.fetch("PB_NATS_SERVER_PAUSE_FILE_PATH", nil)
      end

      def print_subscription_keys
        logger.info "Creating subscriptions:"

        with_each_subscription_key do |subscription_key|
          logger.info "  - #{subscription_key}"
        end
      end

      def subscribe_to_services_once
        with_each_subscription_key do |subscription_key_and_queue|
          subscription_manager.queue_subscribe(subscription_key_and_queue)
        end
      end

      def with_each_subscription_key
        fail ::ArgumentError unless block_given?

        service_klasses.each do |service_klass|
          service_klass.rpcs.each do |service_method, _|
            # Skip unimplemented services.
            next unless service_klass.method_defined?(service_method)
            subscription_key = ::Protobuf::Nats.subscription_key(service_klass, service_method)
            next if do_not_subscribe_to_includes?(subscription_key)
            next unless only_subscribe_to_includes?(subscription_key)

            yield subscription_key
          end
        end
      end

      # Add subscription rounds slowly: subscriptions_per_rpc_endpoint
      # rounds, slow_start_delay seconds apart.
      def finish_slow_start
        logger.info "Slow start has started..."
        completed = 1

        # One round already ran, so only (X - 1) rounds remain.
        (subscriptions_per_rpc_endpoint - 1).times do
          unless @running
            logger.info "Slow start interrupted (server stopping) after #{completed}/#{subscriptions_per_rpc_endpoint} rounds"
            return
          end

          if paused?
            logger.info "Slow start interrupted (server paused) after #{completed}/#{subscriptions_per_rpc_endpoint} rounds"
            return
          end

          completed += 1
          sleep slow_start_delay
          subscribe_to_services_once
          logger.info "Slow start adding another round of subscriptions (#{completed}/#{subscriptions_per_rpc_endpoint})..."
        end

        logger.info "Slow start finished successfully (#{completed}/#{subscriptions_per_rpc_endpoint} rounds completed)."
      end

      def detect_and_handle_a_pause
        @pause_mutex.synchronize do
          case
          # A pause file appeared while we were processing. Unsubscribe.
          when @processing_requests && paused?
            @processing_requests = false
            logger.warn("Pausing server!")
            unsubscribe

          # The pause file is gone. Subscribe again.
          when !@processing_requests && !paused?
            logger.warn("Resuming server: resubscribing to all services and restarting slow start!")
            @processing_requests = true
            subscribe
          end
        end
      end

      def paused?
        !pause_file_path.nil? && ::File.exist?(pause_file_path)
      end

      # nats-pure fires on_close when we called close (normal shutdown), or
      # when the reconnect loop exhausted max_reconnect_attempts on every
      # server. In the second case, the server would otherwise run forever
      # with a dead connection and look healthy while idle. Stop the run
      # loop so a supervisor (systemd/k8s/foreman) restarts the process
      # with a fresh connection. Set max_reconnect_attempts: -1 to retry
      # in-process forever instead; then this never fires for an outage.
      def handle_connection_closed
        return unless @running
        logger.error "Server NATS connection was closed unexpectedly (reconnect attempts exhausted); stopping server so a supervisor can restart it"
        ::Protobuf::Nats.instrument "server.connection_closed"
        stop
      end

      def run
        print_subscription_keys
        if paused?
          yield if block_given?
        else
          subscribe { yield if block_given? }
        end

        loop do
          break unless @running
          begin
            detect_and_handle_a_pause
            instrument_thread_pool_sizes
            instrument_inflight_handlers
            thread_pool.replenish # Respawn workers killed by a non-StandardError.
            subscription_manager.replenish # Same, for intake handler threads.
          rescue => error
            # One failed tick must not end the loop: the drain below would
            # not run, and the server would close with work in flight.
            logger.error "Server supervision tick failed: #{error.class}: #{error.message}"
            ::Protobuf::Nats.notify_error_callbacks(error)
          end
          sleep 1
        end

        unsubscribe

        logger.info "Shutting down subscription manager..."
        begin
          # Do not wrap this in Timeout.timeout. #shutdown already bounds
          # itself with a deadline and non-blocking pushes. Timeout's
          # Thread#raise can fire while a thread holds the SizedQueue
          # mutex; JRuby then hangs the queue trying to unwind through a
          # held mutex. A Timeout wrapper could also fire for real:
          # #shutdown's worst case can exceed 10s past a few handlers
          # (JRuby's default thread count is processor_count).
          subscription_manager.shutdown(5)
        rescue => e
          logger.error "Error during subscription manager shutdown: #{e.message}"
        end

        # Give in-flight handlers time to finish. This timeout tracks
        # handler_overdue_ms, not a fixed 60s, so a legitimate ~60s handler
        # is not killed while its client still waits.
        drain_timeout = shutdown_drain_timeout
        logger.info "Waiting up to #{drain_timeout.round}s for the thread pool to finish shutting down..."
        thread_pool.shutdown
        unless thread_pool.wait_for_termination(drain_timeout)
          abandoned = @inflight.size
          logger.warn "Thread pool did not shut down cleanly within #{drain_timeout.round}s! Abandoned #{abandoned} in-flight handler(s)."
          ::Protobuf::Nats.instrument "server.thread_pool_shutdown_timeout"
          ::Protobuf::Nats.instrument "server.shutdown_abandoned_handlers", abandoned
        end
      ensure
        @stopped = true

        begin
          logger.info "Closing NATS connection..."
          @nats.close if @nats
        rescue => e
          logger.warn "Failed to close NATS connection: #{e.message}"
        end
      end

      def running?
        !@stopped
      end

      def stop
        @running = false
      end

      def subscribe
        subscribe_to_services_once
        yield if block_given?
        finish_slow_start
      end

      def unsubscribe
        logger.info "Unsubscribing from rpc routes..."
        subscription_manager.unsubscribe_all
      end
    end
  end
end
