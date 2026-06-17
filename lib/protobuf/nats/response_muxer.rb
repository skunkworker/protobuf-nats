require 'securerandom'
require "connection_pool"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"
require "protobuf/nats/uuidv7_helper"
require "concurrent"
require "concurrent/collection/timeout_queue"

module Protobuf
  module Nats
    class ResponseMuxer
      LOCK = ::Mutex.new
      MAX_RESPONSES_PER_TOKEN = 10
      TOKEN_TTL_SECONDS = 600 # 10 minutes

      def initialize
        # Per-token response queues for lock-free message delivery. @resp_map is a
        # Concurrent::Map so request threads and dispatcher threads can insert,
        # look up, and delete tokens without serializing on a single mutex (on
        # JRuby it is backed by java.util.concurrent.ConcurrentHashMap). Each
        # value is a Hash { queue:, created_at: }.
        @resp_map = ::Concurrent::Map.new
        @resp_handlers = []
        @cleanup_thread = nil
        @shutdown = false
        @cleanup_mutex = ::Mutex.new
        @cleanup_cv = ::ConditionVariable.new
        @restarting = false  # Flag to prevent concurrent restarts
      end

      def logger
        ::Protobuf::Logging.logger
      end

      # Monotonic clock for token TTL accounting. Cheaper than Time.now (no Time
      # object / timezone work per request) and immune to wall-clock jumps.
      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Number of dispatcher threads draining the response subscription. On JRuby
      # (true parallelism) a single dispatcher is a hard throughput ceiling, so we
      # fan out to processor_count; on CRuby the GVL makes extra dispatchers
      # pointless, so we stay at 1. Overridable via env for tuning/tests.
      def dispatcher_count
        @dispatcher_count ||= begin
          if ::ENV.key?("PB_NATS_RESPONSE_MUXER_DISPATCHERS")
            [::ENV["PB_NATS_RESPONSE_MUXER_DISPATCHERS"].to_i, 1].max
          elsif ::RUBY_ENGINE == "jruby"
            [::Concurrent.processor_count, 1].max
          else
            1
          end
        end
      end

      def cleanup(token)
        # Atomic remove-and-return; close the queue to wake any waiting threads.
        entry = @resp_map.delete(token)
        entry[:queue]&.close if entry
      end

      def next_message(token, timeout)
        # Lock-free get of the per-token queue.
        entry = @resp_map[token]
        queue = entry && entry[:queue]

        unless queue
          logger.warn "Token #{token} not found or already cleaned up during next_message"
          raise ::NATS::Timeout
        end

        # Handle edge case: zero or negative timeout
        if timeout && timeout <= 0
          raise ::NATS::Timeout
        end

        # Use TimeoutQueue's native timeout support for efficient, lock-free waiting per token
        # Each token has its own queue, eliminating contention between different requests
        begin
          # TimeoutQueue.pop(non_block, timeout: seconds)
          # - With timeout: blocks until message arrives or timeout expires (returns nil on timeout)
          # - Without timeout (nil): blocks indefinitely until message arrives
          msg = if timeout
                  queue.pop(false, timeout: timeout)
                else
                  queue.pop(false)
                end

          # Queue.pop returns nil when:
          # 1. The queue is closed
          # 2. The timeout expires
          unless msg
            logger.warn "Queue closed or timeout for token #{token} during next_message"
            raise ::NATS::Timeout
          end

          msg
        rescue ThreadError
          # Queue was closed - treat as timeout
          logger.warn "Queue closed for token #{token} during next_message"
          raise ::NATS::Timeout
        end
      end

      def new_request
        # Use UUIDv7 so we can figure out what time a message was originally created in-memory.
        token = UUIDv7Helper.generate # nats.new_inbox with nuid is not threadsafe.

        # Create a dedicated queue for this token. Concurrent::Map#[]= is atomic,
        # so no surrounding lock is required.
        @resp_map[token] = {
          queue: ::Concurrent::Collection::TimeoutQueue.new,
          created_at: monotonic_now
        }

        ResponseMuxerRequest.new(self, token)
      end

      def publish(subject, data, token)
        # Validate muxer started before publish
        unless @resp_inbox_prefix
          raise ::Protobuf::Nats::Errors::ResponseMuxer, "ResponseMuxer not started - cannot publish"
        end

        nats = Protobuf::Nats.client_nats_connection
        reply_to = "#{@resp_inbox_prefix}.#{token}"
        nats.publish(subject, data, reply_to)
      end

      def restart
        logger.debug "restarting response_muxer"

        # Prevent concurrent restarts - only one restart at a time
        LOCK.synchronize do
          if @restarting
            logger.warn "Restart already in progress, skipping concurrent restart request"
            return
          end
          @restarting = true
        end

        # Yield so other restart callers spawned around the same time get a
        # chance to reach the @restarting check above and skip. Without this,
        # CRuby's GVL can let the current thread run the entire restart to
        # completion (clearing @restarting) before sibling threads even enter
        # the method, defeating the concurrent-restart guard.
        Thread.pass

        begin
          # Stop the existing muxer first, if it's running
          LOCK.synchronize do
            @resp_handlers.each(&:kill)
            @resp_handlers.clear
            if @resp_sub
              begin
                @resp_sub.unsubscribe
              rescue => e
                logger.warn "Failed to unsubscribe old response muxer subscription: #{e.message}"
              ensure
                # Always set to nil, even if unsubscribe raises
                @resp_sub = nil
              end
            end

            # Stop the cleanup thread
            stop_cleanup_thread

            @started = false
          end

          # Then start it fresh.
          start
        ensure
          # Always clear the restarting flag
          LOCK.synchronize { @restarting = false }
        end
      end

      def start
        return if started?
        LOCK.synchronize do
          # We check this twice in case another thread was waiting for the lock to
          # start this party. Use the unlocked check to prevent deadlocks.
          return if _started?

          nats = ::Protobuf::Nats.client_nats_connection
          return if nats.nil?

          # Clean up partial state on exception
          begin
            @resp_inbox_prefix = nats.new_inbox

            # Subscribe to our per-instance inbox
            @resp_sub = nats.subscribe("#{@resp_inbox_prefix}.*")
            @started = true
          rescue => e
            # Clean up partial state
            @resp_inbox_prefix = nil
            @resp_sub = nil
            @started = false
            logger.error "Failed to start ResponseMuxer: #{e.message}"
            raise
          end
        end

        # Start the cleanup thread
        start_cleanup_thread

        # Top up the dispatcher pool to dispatcher_count. Prunes dead threads
        # first so self-healing restarts converge to the target count instead of
        # multiplying threads.
        LOCK.synchronize do
          @resp_handlers.select!(&:alive?)
          @resp_handlers << spawn_dispatcher while @resp_handlers.size < dispatcher_count
        end
      end

      def started?
        LOCK.synchronize { _started? }
      end

      # Periodic cleanup of stale tokens
      def cleanup_stale_tokens
        cutoff = monotonic_now - TOKEN_TTL_SECONDS

        # Collect stale tokens first, then delete. Concurrent::Map iteration does
        # not hold a global lock, so request threads are never blocked across this
        # O(n) scan (unlike the previous single-mutex implementation).
        stale_tokens = []
        @resp_map.each_pair do |token, data|
          created_at = data[:created_at]
          stale_tokens << token if created_at && created_at < cutoff
        end

        stale_count = 0
        stale_tokens.each do |token|
          data = @resp_map.delete(token)
          next unless data
          stale_count += 1
          logger.warn "Cleaning up stale token #{token} created at #{data[:created_at]}"
          # Close the queue to wake any waiting threads
          data[:queue]&.close
        end

        if stale_count > 0
          ::ActiveSupport::Notifications.instrument "response_muxer.stale_tokens_cleaned.protobuf-nats", stale_count
        end
      end

      # Stop the cleanup thread
      def stop
        LOCK.synchronize do
          stop_cleanup_thread
          @resp_handlers.each(&:kill)
          @resp_handlers.clear
          if @resp_sub
            begin
              @resp_sub.unsubscribe
            rescue => e
              logger.warn "Failed to unsubscribe during stop: #{e.message}"
            ensure
              @resp_sub = nil
            end
          end
          @started = false
        end
      end

      private

      def _started?
        !!@started
      end

      # Spawn a single dispatcher thread. Multiple dispatchers safely share the
      # one @resp_sub.pending_queue (Queue is thread-safe) and route via the
      # lock-free @resp_map.
      def spawn_dispatcher
        Thread.new do
          # Unique thread name for debugging
          Thread.current.name = "response-muxer-#{Thread.current.object_id}"
          begin
            # Reset crash count on successful start
            @crash_count = 0
            run_dispatch_loop
          rescue => fatal_error
            # Only truly fatal errors that kill the loop reach here (ThreadError
            # from the shared pending_queue being closed).
            logger.error("ResponseMuxer thread crashed fatally. Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error)

            # --- Self-healing logic ---
            @crash_count = (@crash_count || 0) + 1
            # Exponential backoff, e.g., 1, 4, 9, 16s... capped at 60s.
            sleep_duration = [(@crash_count**2), 60].min
            logger.warn("Waiting #{sleep_duration}s before attempting to restart ResponseMuxer.")
            sleep sleep_duration
            # --- End of self-healing logic ---

            # After sleeping, reset the state and try to start again.
            LOCK.synchronize do
              if @resp_sub
                begin
                  @resp_sub.unsubscribe
                rescue => e
                  logger.warn "Failed to unsubscribe old response muxer subscription during self-healing: #{e.message}"
                ensure
                  @resp_sub = nil
                end
              end
              @started = false
            end
            start
          end
        end
      end

      def run_dispatch_loop
        loop do
          begin
            # --- Start of per-message block ---
            msg = @resp_sub.pending_queue.pop

            # ACK means the message has been picked up and put into the waiting thread_pool
            next if msg.nil?

            # Decrease pending size since consumed already.
            # NOTE: advisory metric only; with multiple dispatchers this is a
            # benign lost-update race on the NATS subscription's counter.
            @resp_sub.pending_size -= msg.data.size if @resp_sub

            dispatch_message(msg)
            # --- End of per-message block ---
          rescue => per_message_error
            # ThreadError is fatal, it means the queue is closed and the loop cannot continue.
            raise if per_message_error.is_a?(::ThreadError)

            # Log the error for the specific message, but DON'T kill the thread.
            logger.error("ResponseMuxer failed to process a message. Error: #{per_message_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(per_message_error)
          end
        end
      end

      def dispatch_message(msg)
        # Validate message subject before processing
        unless msg.subject.is_a?(String) && msg.subject.include?('.')
          ::ActiveSupport::Notifications.instrument "client.invalid_message.protobuf-nats", 1

          logger.warn "Received message with invalid subject: #{msg.subject}. Dropping."
          return
        end

        # example(random data):
        # _INBOX.{random_data}.{random_data_msg_id}
        token = msg.subject.split('.').last

        logger.debug { "token: #{token}, resp_map.keys:#{@resp_map.keys}" } if logger.debug?

        # Lock-free get of the per-token queue.
        entry = @resp_map[token]
        queue = entry && entry[:queue]

        unless queue
          # Try to decode the UUIDv7 timestamp to calculate message age
          delay_seconds = UUIDv7Helper.age_in_seconds(token)

          ::ActiveSupport::Notifications.instrument "client.unexpected_message.protobuf-nats", delay_seconds || 1

          if delay_seconds
            logger.warn "Received unexpected message (#{delay_seconds.round(3)}s old). MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject rescue 'unknown'}. Dropping unexpected message."
          else
            logger.warn "Received unexpected message. MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject rescue 'unknown'}. Dropping unexpected message."
          end
          return
        end

        # Push message onto the queue - this is lock-free and thread-safe.
        begin
          # Check queue size to prevent memory bloat
          if queue.size >= MAX_RESPONSES_PER_TOKEN
            logger.warn "Token #{token} has #{queue.size} queued responses. Possible duplicate messages or slow consumer. Dropping message."
            return
          end

          queue.push(msg)
        rescue ThreadError
          # Queue was closed (cleanup happened) - this is fine, just drop the message
          logger.debug "Queue closed for token #{token}, dropping message"
        end
      end

      def start_cleanup_thread
        # Only start if not already running
        return if @cleanup_thread&.alive?

        @cleanup_mutex.synchronize { @shutdown = false }
        @cleanup_thread = Thread.new do
          begin
            loop do
              # Wait for 60 seconds or until signaled to shutdown
              @cleanup_mutex.synchronize do
                @cleanup_cv.wait(@cleanup_mutex, 60) unless @shutdown
              end

              break if @cleanup_mutex.synchronize { @shutdown }

              begin
                cleanup_stale_tokens
              rescue => error
                logger.error("ResponseMuxer cleanup thread error: #{error.message}")
                ::Protobuf::Nats.notify_error_callbacks(error)
              end
            end
          rescue => fatal_error
            logger.error("ResponseMuxer cleanup thread crashed: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error)
          end
        end
        # Name the thread from the outside so the name is visible to callers
        # immediately after start_cleanup_thread returns (no race with the
        # thread body executing).
        @cleanup_thread.name = "response-muxer-cleanup-#{object_id}"
      end

      def stop_cleanup_thread
        if @cleanup_thread&.alive?
          @cleanup_mutex.synchronize do
            @shutdown = true
            @cleanup_cv.signal # Wake up the cleanup thread immediately
          end
          # Should exit almost immediately now
          @cleanup_thread.join(0.5)
          # Force kill if still alive (shouldn't happen)
          @cleanup_thread.kill if @cleanup_thread&.alive?
        end
        @cleanup_thread = nil
      end
    end
  end
end
