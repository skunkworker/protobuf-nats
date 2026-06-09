require 'securerandom'
require "connection_pool"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

module Protobuf
  module Nats
    class ResponseMuxer
      LOCK = ::Mutex.new
      MAX_RESPONSES_PER_TOKEN = 10
      TOKEN_TTL_SECONDS = 600 # 10 minutes

      def initialize
        @resp_map = Hash.new { |h,k| h[k] = { } }
        @resp_handlers = []
        @monitor = ::Monitor.new
        @prng_lock = ::Mutex.new
        @prng = Random.new
        @cleanup_thread = nil
        @shutdown = false
        @cleanup_mutex = ::Mutex.new
        @cleanup_cv = ::ConditionVariable.new
        @restarting = false  # Flag to prevent concurrent restarts
      end

      def logger
        ::Protobuf::Logging.logger
      end

      def cleanup(token)
        @monitor.synchronize { @resp_map.delete(token) }
      end

      def next_message(token, timeout)
        # Calculate the deadline once, up front.
        end_time = Time.now + timeout if timeout

        @monitor.synchronize do
          # Loop as long as no message is available.
          while !(@resp_map[token].key?(:response) && !@resp_map[token][:response].empty?)
            # On each loop, calculate the time remaining until the deadline.
            remaining = end_time ? end_time - Time.now : nil

            # If time has run out, we must raise a timeout error. This is the
            # definitive exit condition for the loop.
            raise ::NATS::Timeout if timeout && remaining <= 0

            # Guard against deleted tokens
            signal = @resp_map[token][:signal]
            unless signal
              logger.warn "Token #{token} not found or already cleaned up during next_message"
              raise ::NATS::Timeout # Treat as timeout to maintain backward compatibility
            end

            # Wait only for the time remaining. If the wait is woken up
            # spuriously, the loop repeats, 'remaining' is recalculated
            # (now smaller), and we wait again for the correct shorter duration.
            signal.wait(remaining)
          end

          # This line is only reached if a message was successfully received.
          @resp_map[token][:response].shift
        end
      end

      def new_uuidv7
        # Thread-safe PRNG access
        @prng_lock.synchronize { @prng.uuid_v7(extra_timestamp_bits: 12) }
      end

      def new_request
        # Use UUIDv7 so we can figure out what time a message was originally created in-memory.
        token = new_uuidv7 # nats.new_inbox with nuid is not threadsafe.

        @monitor.synchronize do
          @resp_map[token][:signal] = @monitor.new_cond
          @resp_map[token][:created_at] = Time.now
        end

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

        LOCK.synchronize do
          @resp_handlers.select!(&:alive?)
          @resp_handlers << Thread.new do
            # Unique thread name for debugging
            Thread.current.name = "response-muxer-#{Thread.current.object_id}"
            begin
              # Reset crash count on successful start
              @crash_count = 0

              loop do
                begin
                  # --- Start of per-message block ---
                  msg = @resp_sub.pending_queue.pop

                  # ACK means the message has been picked up and put into the waiting thread_pool
                  next if msg.nil?

                  @monitor.synchronize do
                    # Decrease pending size since consumed already
                    @resp_sub.pending_size -= msg.data.size if @resp_sub

                    # Validate message subject before processing
                    unless msg.subject.is_a?(String) && msg.subject.include?('.')
                      ::ActiveSupport::Notifications.instrument "client.invalid_message.protobuf-nats", 1

                      logger.warn "Received message with invalid subject: #{msg.subject}. Dropping."
                      next
                    end

                    # example(random data):
                    # _INBOX.{random_data}.{random_data_msg_id}
                    token = msg.subject.split('.').last

                    logger.debug { "token: #{token}, resp_map.keys:#{@resp_map.keys}" }

                    unless @resp_map.key?(token)
                      ::ActiveSupport::Notifications.instrument "client.unexpected_message.protobuf-nats", 1

                      logger.warn "Received unexpected message. MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject rescue 'unknown'}. Dropping unexpected message."

                      # NOTE: use #next instead of a #break here
                      # We want to move onto the next message quickly, rather than escaping from the outer `loop do` loop.
                      next
                    end

                    signal = @resp_map[token][:signal]
                    @resp_map[token][:response] ||= []

                    # Limit response array size
                    if @resp_map[token][:response].size >= MAX_RESPONSES_PER_TOKEN
                      logger.warn "Token #{token} has #{@resp_map[token][:response].size} queued responses. Possible duplicate messages or slow consumer. Dropping oldest."
                      @resp_map[token][:response].shift # Remove oldest
                    end

                    @resp_map[token][:response] << msg
                    signal.signal

                    # Metrics for monitoring
                    ::ActiveSupport::Notifications.instrument "response_muxer.token_count.protobuf-nats", @resp_map.size
                  end
                  # --- End of per-message block ---
                rescue => per_message_error
                  # ThreadError is fatal, it means the queue is closed and the loop cannot continue.
                  raise if per_message_error.is_a?(::ThreadError)

                  # Log the error for the specific message, but DON'T kill the thread.
                  logger.error("ResponseMuxer failed to process a message. Error: #{per_message_error.message}")
                  ::Protobuf::Nats.notify_error_callbacks(per_message_error)
                end
              end
            rescue => fatal_error
              # This block is now only for truly fatal errors that kill the loop itself.
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
      end

      def started?
        LOCK.synchronize { _started? }
      end

      # Periodic cleanup of stale tokens
      def cleanup_stale_tokens
        cutoff = Time.now - TOKEN_TTL_SECONDS

        @monitor.synchronize do
          stale_count = 0
          @resp_map.delete_if do |token, data|
            if data[:created_at] && data[:created_at] < cutoff
              stale_count += 1
              logger.warn "Cleaning up stale token #{token} created at #{data[:created_at]}"
              true
            else
              false
            end
          end

          if stale_count > 0
            ::ActiveSupport::Notifications.instrument "response_muxer.stale_tokens_cleaned.protobuf-nats", stale_count
          end
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

      def start_cleanup_thread
        # Only start if not already running
        return if @cleanup_thread&.alive?

        @cleanup_mutex.synchronize { @shutdown = false }
        @cleanup_thread = Thread.new do
          Thread.current.name = "response-muxer-cleanup-#{object_id}"
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
