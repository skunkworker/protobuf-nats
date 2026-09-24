require "active_support"
require "active_support/core_ext/class/subclasses"
require "concurrent"
require "protobuf/rpc/server"
require "protobuf/rpc/service"
require "protobuf/nats/thread_pool"
require "protobuf/nats/byte_bounded_queue"

module Protobuf
  module Nats
    class SuperSubscriptionManager
      def initialize(nats, &cb)
        # Shared queue for all subscriptions, bounded by count and bytes
        # (see `intake_queue_size` / `intake_queue_bytes`). Byte drops
        # report as `server.intake_bytes_dropped`.
        @pending_queue = ::Protobuf::Nats::ByteBoundedQueue.new(
          intake_queue_size, intake_queue_bytes,
          :on_drop => lambda { |bytes| ::Protobuf::Nats.instrument("server.intake_bytes_dropped", bytes) }
        )
        @subscriptions = []
        @subscriptions_mutex = ::Mutex.new
        @nats = nats
        @callback = cb

        # Several handler threads process intake: one thread caps throughput
        # on JRuby, and a slow callback ACK would block every other subject.
        @pending_queue_handlers = handler_count.times.map { |i| spawn_handler(i) }

        ::Protobuf::Nats.instrument("server.subscription_handler_count", @pending_queue_handlers.size)
      end

      def logger
        ::Protobuf::Logging.logger
      end

      # Handler thread count: `processor_count` on JRuby (true parallelism),
      # 1 on CRuby (the GVL makes more useless). Env var overrides for
      # tuning or tests. Matches `ResponseMuxer#dispatcher_count`.
      def handler_count
        @handler_count ||= begin
          default = ::RUBY_ENGINE == "jruby" ? ::Concurrent.processor_count : 1
          ::Protobuf::Nats.env_int("PB_NATS_SERVER_SUBSCRIPTION_HANDLERS", default, :min => 1)
        end
      end

      # Capacity of the shared intake queue. nats-pure's default (65,536)
      # lets requests wait past a client's `ack_timeout`, so a deep backlog
      # is mostly abandoned work. A smaller size trades that for fast drops
      # and retries. Kept at the nats-pure default; tune down with
      # `PB_NATS_SERVER_STALE_REQUEST_MS`.
      def intake_queue_size
        @intake_queue_size ||= ::Protobuf::Nats.env_int("PB_NATS_SERVER_INTAKE_QUEUE_SIZE", ::NATS::IO::DEFAULT_SUB_PENDING_MSGS_LIMIT, :min => 1)
      end

      # Byte limit for the shared intake queue: message count alone can't
      # bound heap use (65,536 large requests is a lot of heap).
      # `ByteBoundedQueue` drops a message over this limit rather than
      # blocking nats-pure's read thread. Default 128 MiB: higher than the
      # client muxer's 64 MiB, since the server fans out to more handlers.
      DEFAULT_INTAKE_QUEUE_BYTES = 128 * 1024 * 1024 # 128MiB

      # Read once in `#initialize`; no memoization needed.
      def intake_queue_bytes
        ::Protobuf::Nats.env_int("PB_NATS_SERVER_INTAKE_QUEUE_BYTES", DEFAULT_INTAKE_QUEUE_BYTES, :min => 1)
      end

      def queue_subscribe(name)
        logger.debug { "queue_subscribe(#{name})" }
        sub = @nats.subscribe(name, :queue => name)

        # See `Protobuf::Nats.disable_subscription_byte_limit!` for why.
        ::Protobuf::Nats.disable_subscription_byte_limit!(sub)

        existing_pending_queue = sub.pending_queue
        sub.pending_queue = @pending_queue

        # Match the SlowConsumer limit to the shared queue's capacity.
        # nats-pure's read thread drops a message only when
        # `pending_queue.size >= pending_msgs_limit`; else it pushes, even
        # into a full queue, blocking the read thread and PING/PONG. The
        # sub's default (65,536) sits above a smaller tuned queue, so that
        # never fires. limit == capacity trips the drop before a push blocks.
        sub.pending_msgs_limit = intake_queue_size if sub.respond_to?(:pending_msgs_limit=)

        # Move any messages already on the old queue to the new one: one can
        # land there in the brief window before this swap (rare, not zero).
        migrated_count = 0
        max_migrations = 10000  # Safety limit

        while !existing_pending_queue.empty? && migrated_count < max_migrations
          # Non-blocking pop: another consumer could drain this queue too.
          begin
            msg = existing_pending_queue.pop(true)
          rescue ThreadError
            break
          end

          # See `#push_with_deadline` for why not `Timeout.timeout`.
          if push_with_deadline(msg, 1)
            migrated_count += 1
            logger.warn "Migrated message #{migrated_count} from old queue to central queue"
          else
            logger.error "Failed to migrate message to central queue (queue full), dropping message"
            break
          end
        end

        if migrated_count >= max_migrations
          logger.error "Hit migration limit! Old queue still has #{existing_pending_queue.size} messages"
        end

        @subscriptions_mutex.synchronize { @subscriptions << sub }

        sub
      end

      def shutdown(timeout = 5)
        handlers = @pending_queue_handlers.select(&:alive?)
        return if handlers.empty?

        # Send each handler its own poison pill to wake it.
        handlers.size.times do
          # Clear space if the queue is full, so the pill fits.
          if @pending_queue.num_waiting.zero? && @pending_queue.size >= @pending_queue.max
            logger.warn "Queue full during shutdown, clearing to make room for shutdown signal"
            @pending_queue.clear rescue nil
          end

          # See `#push_with_deadline` for why not `Timeout.timeout`.
          unless push_with_deadline(:shutdown, 1)
            logger.error "Failed to send shutdown signal (queue blocked); will force-kill remaining handlers"
            break
          end
        end

        # Join all handlers within one shared deadline. Force-kill stragglers.
        deadline = monotonic + timeout
        handlers.each do |handler|
          remaining = deadline - monotonic
          handler.join(remaining.positive? ? remaining : 0)
        end

        handlers.each do |handler|
          next unless handler.alive?
          logger.warn "Handler thread did not shut down in time, forcefully killing..."
          handler.kill
          handler.join(1) rescue nil
        end

        @pending_queue.clear rescue nil
      end

      # Intake backpressure gauge: depth of the shared intake queue.
      def pending_queue_size
        @pending_queue.size
      end

      # Heap backpressure gauge: resident bytes in the shared intake queue.
      def pending_queue_bytes
        @pending_queue.bytesize
      end

      def unsubscribe_all
        # Take and clear the list: pause/resume re-subscribes from scratch,
        # so keeping old entries would grow it forever and re-unsubscribe
        # already-dead subscriptions on every later pause.
        subscriptions = @subscriptions_mutex.synchronize do
          subs = @subscriptions.dup
          @subscriptions.clear
          subs
        end
        subscriptions.each do |sub|
          begin
            sub.unsubscribe
          rescue => e
            logger.warn "Failed to unsubscribe #{sub.subject rescue 'unknown'}: #{e.message}"
          end
        end
      end

      private

      def monotonic
        ::Protobuf::Nats.monotonic_time
      end

      # Push with a deadline, without `Timeout.timeout`: its async
      # `Thread#raise` is unsafe around `SizedQueue#push`'s internal mutex.
      # On JRuby 10.x a timeout mid-push raises `ThreadError: Attempt to
      # unlock a mutex which is locked by another thread/fiber` instead of
      # `Timeout::Error`, so shutdown or migration fails. CRuby unwinds
      # cleanly, so only JRuby hits this. Poll a non-blocking push against a
      # monotonic deadline instead. Returns true if pushed, false on
      # deadline or a closed queue.
      def push_with_deadline(obj, timeout)
        deadline = monotonic + timeout
        loop do
          begin
            @pending_queue.push(obj, true) # non_block: raises ThreadError when full
            return true
          rescue ::ClosedQueueError
            return false
          rescue ::ThreadError
            return false if monotonic >= deadline
            sleep 0.01
          end
        end
      end

      # Spawn one intake handler with its own `crash_count`: a shared counter
      # would lose updates across handlers on JRuby's true parallelism. The
      # counter decays to zero on the next processed message, so a later
      # crash restarts the backoff from 1 second.
      def spawn_handler(index)
        ::Thread.new do
          ::Thread.current.name = "subscription-manager-#{object_id}-#{index}"
          crash_count = 0

          begin
            loop do
              msg = nil
              begin
                msg = @pending_queue.pop

                # nil means the queue closed (e.g. nats-pure closes the
                # swapped sub queue on connection close). It pops nil
                # forever, so park briefly instead of looping on the rescue.
                if msg.nil?
                  sleep ::Protobuf::Nats::CLOSED_QUEUE_PARK_SECONDS
                  next
                end

                # Stop on the shutdown poison pill.
                break if msg == :shutdown

                @callback.call(msg.data, msg.reply, msg.subject)
                crash_count = 0 unless crash_count.zero? # healthy: decay the backoff
              rescue => per_message_error
                # Log this message's error; keep the thread running.
                logger.error("SubscriptionManager failed to process message: #{msg.inspect rescue 'unknown'}. Error: #{per_message_error.message}")
                ::Protobuf::Nats.notify_error_callbacks(per_message_error) rescue nil
              end
            end
          rescue => fatal_error
            raise if fatal_error.is_a?(SystemExit) || fatal_error.is_a?(Interrupt) || fatal_error.is_a?(SignalException)

            logger.error("SubscriptionManager handler crashed fatally! Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error) rescue nil
            ::Protobuf::Nats.instrument("server.subscription_handler_crashed", 1) rescue nil

            # Self-heal with exponential backoff (counter is per thread).
            crash_count += 1
            sleep_duration = ::Protobuf::Nats.crash_backoff_seconds(crash_count)
            logger.warn("Waiting #{sleep_duration}s before restarting SubscriptionManager handler...")
            sleep sleep_duration

            retry  # Restart the loop.
          end
        end
      end
    end
  end
end
