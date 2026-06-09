require "active_support"
require "active_support/core_ext/class/subclasses"
require "protobuf/rpc/server"
require "protobuf/rpc/service"
require "protobuf/nats/thread_pool"

module Protobuf
  module Nats
    class SuperSubscriptionManager
      def initialize(nats, &cb)
        # Central queue used by all subscriptions
        @pending_queue = ::SizedQueue.new(::NATS::IO::DEFAULT_SUB_PENDING_MSGS_LIMIT)
        @subscriptions = []
        @nats = nats
        @callback = cb
        @crash_count = 0

        @pending_queue_handler = Thread.new do
          Thread.current.name = "subscription-manager-#{object_id}"
          begin
            @crash_count = 0  # Reset on successful start

            loop do
              msg = nil
              begin
                # --- Per-message processing ---
                msg = @pending_queue.pop
                # Check for shutdown poison pill
                break if msg == :shutdown

                @callback.call(msg.data, msg.reply, msg.subject)
                # --- End per-message processing ---
              rescue => per_message_error
                # Log the error for the specific message, but DON'T kill the thread.
                logger.error("SubscriptionManager failed to process message: #{msg.inspect rescue 'unknown'}. Error: #{per_message_error.message}")
                ::Protobuf::Nats.notify_error_callbacks(per_message_error) rescue nil
              end
            end
          rescue => fatal_error
            raise if fatal_error.is_a?(SystemExit) || fatal_error.is_a?(Interrupt) || fatal_error.is_a?(SignalException)

            # This block is for fatal errors that crash the thread itself.
            logger.error("SubscriptionManager handler crashed fatally! Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error) rescue nil

            # Self-healing with exponential backoff
            @crash_count += 1
            sleep_duration = [(@crash_count**2), 60].min
            logger.warn("Waiting #{sleep_duration}s before restarting SubscriptionManager handler...")
            sleep sleep_duration

            retry  # Restart the loop
          end
        end
      end

      def logger
        ::Protobuf::Logging.logger
      end

      def queue_subscribe(name)
        logger.debug "queue_subscribe(#{name})"
        sub = @nats.subscribe(name, :queue => name)

        # Create a subscription but reset the pending queue to use a central pending queue.
        existing_pending_queue = sub.pending_queue
        sub.pending_queue = @pending_queue

        # Push all race-conditioned messages onto the pending queue.
        # Should address a potential race condition. Chances of the round-trip message to an
        # existing queue before this queue swap happens seems extremely low, but possible.
        migrated_count = 0
        max_migrations = 10000  # Safety limit

        while !existing_pending_queue.empty? && migrated_count < max_migrations
          msg = existing_pending_queue.pop

          # Non-blocking push with timeout
          begin
            Timeout.timeout(1) do
              @pending_queue << msg
            end
            migrated_count += 1
            logger.warn "Migrated message #{migrated_count} from old queue to central queue"
          rescue Timeout::Error
            logger.error "Failed to migrate message to central queue (queue full), dropping message"
            break
          end
        end

        if migrated_count >= max_migrations
          logger.error "Hit migration limit! Old queue still has #{existing_pending_queue.size} messages"
        end

        @subscriptions << sub

        sub
      end

      def shutdown(timeout = 5)
        # Check if thread is alive first
        return unless @pending_queue_handler&.alive?

        # Non-blocking push of shutdown signal
        begin
          # Clear some space if queue is full
          if @pending_queue.num_waiting == 0 && @pending_queue.size >= @pending_queue.max
            logger.warn "Queue full during shutdown, clearing to make room for shutdown signal"
            @pending_queue.clear rescue nil
          end

          Timeout.timeout(1) do
            @pending_queue << :shutdown
          end
        rescue Timeout::Error
          logger.error "Failed to send shutdown signal (queue blocked), force killing thread"
          @pending_queue_handler.kill if @pending_queue_handler&.alive?
          return
        end

        # Handle timeout and force kill if needed
        unless @pending_queue_handler.join(timeout)
          logger.warn "Handler thread did not shutdown within #{timeout}s, forcefully killing..."
          @pending_queue_handler.kill
          @pending_queue_handler.join(1) rescue nil
        end

        # Clean up queue
        @pending_queue.clear rescue nil
      end

      def unsubscribe_all
        @subscriptions.each do |sub|
          begin
            sub.unsubscribe
          rescue => e
            logger.warn "Failed to unsubscribe #{sub.subject rescue 'unknown'}: #{e.message}"
          end
        end
      end
    end
  end
end
