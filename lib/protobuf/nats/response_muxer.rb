require 'securerandom'
require "connection_pool"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

module Protobuf
  module Nats
    class ResponseMuxer
      LOCK = ::Mutex.new

      def initialize
        @resp_map = Hash.new { |h,k| h[k] = { } }
        @resp_handlers = []
      end

      def logger
        ::Protobuf::Logging.logger
      end

      def cleanup(token)
        @resp_sub.synchronize { @resp_map.delete(token) }
      end

      def next_message(token, timeout)
        # Calculate the deadline once, up front.
        end_time = Time.now + timeout if timeout

        @resp_sub.synchronize do
          # Loop as long as no message is available.
          while !(@resp_map[token].key?(:response) && !@resp_map[token][:response].empty?)
            # On each loop, calculate the time remaining until the deadline.
            remaining = end_time ? end_time - Time.now : nil

            # If time has run out, we must raise a timeout error. This is the
            # definitive exit condition for the loop.
            raise ::NATS::Timeout if timeout && remaining <= 0

            # Wait only for the time remaining. If the wait is woken up
            # spuriously, the loop repeats, 'remaining' is recalculated
            # (now smaller), and we wait again for the correct shorter duration.
            @resp_map[token][:signal].wait(remaining)
          end

          # This line is only reached if a message was successfully received.
          @resp_map[token][:response].shift
        end
      end

      def new_request
        token = ::SecureRandom.uuid # nats.new_inbox with nuid is not threadsafe.

        @resp_sub.synchronize do
          @resp_map[token][:signal] = @resp_sub.new_cond
        end

        ResponseMuxerRequest.new(self, token)
      end

      def publish(subject, data, token)
        nats = Protobuf::Nats.client_nats_connection
        reply_to = "#{@resp_inbox_prefix}.#{token}"
        nats.publish(subject, data, reply_to)
      end

      def restart
        logger.debug "restarting response_muxer"

        # Stop the existing muxer first, if it's running
        LOCK.synchronize do
          @resp_handlers.each(&:kill)
          @resp_handlers.clear
          @started = false
        end

        # Then start it fresh.
        start
      end

      def start
        return if started?
        LOCK.synchronize do
          # We check this twice in case another thread was waiting for the lock to
          # start this party. Use the unlocked check to prevent deadlocks.
          return if _started?

          nats = ::Protobuf::Nats.client_nats_connection
          return if nats.nil?

          @resp_inbox_prefix = nats.new_inbox

          # Subscribe to our per-instance inbox
          @resp_sub = nats.subscribe("#{@resp_inbox_prefix}.*")
          @started = true
        end

        @resp_handlers << Thread.new do
          Thread.current.name = "response-muxer"
          begin
            loop do
              begin
                # --- Start of per-message block ---
                msg = @resp_sub.pending_queue.pop

                # ACK means the message has been picked up and put into the waiting thread_pool
                next if msg.nil?

                @resp_sub.synchronize do
                  # Decrease pending size since consumed already
                  @resp_sub.pending_size -= msg.data.size

                  # example(random data):
                  # _INBOX.{random_data}.{random_data_msg_id}
                  token = msg.subject.split('.').last

                  logger.debug "token: #{token}, resp_map.keys:#{@resp_map.keys}"

                  unless @resp_map.key?(token)
                    ::ActiveSupport::Notifications.instrument "client.unexpected_message.protobuf-nats", 1

                    logger.warn "Received unexpected message. MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject}. Dropping unexpected message."

                    # NOTE: use #next instead of a #break here
                    # We want to move onto the next message quickly, rather than escaping from the outer `loop do` loop.
                    next
                  end

                  signal = @resp_map[token][:signal]
                  @resp_map[token][:response] ||= []
                  @resp_map[token][:response] << msg
                  signal.signal
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
            LOCK.synchronize { @started = false }
            start
          end
        end
      end

      def started?
        LOCK.synchronize { _started? }
      end

      private

      def _started?
        !!@started
      end
    end
  end
end
