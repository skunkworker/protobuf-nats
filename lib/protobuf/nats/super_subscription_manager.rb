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

        @pending_queue_handler = Thread.new do
          Thread.current.name = "subscription-manager"
          begin
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
            logger.error("The SubscriptionManager's handler thread has crashed fatally! Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error) rescue nil
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

        while !existing_pending_queue.empty?
          logger.warn "found message(s) when trying to queue_subscribe, shoveling them onto the main @pending_queue"
          @pending_queue << existing_pending_queue.pop
        end

        @subscriptions << sub

        sub
      end

      def shutdown(timeout = 5)
        # Send poison pill and wait for thread to finish
        @pending_queue << :shutdown
        @pending_queue_handler.join(timeout)
      end

      def unsubscribe_all
        @subscriptions.each { |sub| sub.unsubscribe }
      end
    end
  end
end
