require 'securerandom'
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

      # The response queue caps message count and bytes. nats-pure drops
      # messages (SlowConsumer) when either limit trips, so memory stays
      # bounded instead of filling the JVM heap (caused an OOM in 0.13.2).
      # Dispatchers keep the queue near empty, so these limits bound bursts.
      #
      # The count is lower than nats-pure (65,536) or nats.go (500,000),
      # because those bound off-heap buffers, not Ruby heap objects. The byte
      # limit is the real ceiling, set to 64 MiB to match both clients.
      # Override with PB_NATS_RESPONSE_MUXER_QUEUE_SIZE and _QUEUE_BYTES.
      DEFAULT_RESPONSE_QUEUE_SIZE = 1024
      DEFAULT_RESPONSE_QUEUE_BYTES = 64 * 1024 * 1024 # 64MiB

      # Sentinel pushed onto a token's queue to wake a waiter in next_message.
      # On JRuby, Queue#close does not wake a timed pop (native Queue on JRuby
      # 10, concurrent-ruby's RubyTimeoutQueue on JRuby 9.4); CRuby's close
      # does. Pushing this sentinel wakes the waiter on every engine;
      # next_message treats it as a timeout.
      QUEUE_WAKE = ::Object.new

      # Thread-local key naming the subscription a dispatcher is draining
      # (see run_dispatch_loop and spawn_dispatcher).
      DISPATCHING_SUB_KEY = :pb_nats_dispatching_sub

      def initialize
        # @resp_map is a Concurrent::Map, so request and dispatcher threads
        # insert, read, and delete tokens without one shared mutex (backed by
        # java.util.concurrent.ConcurrentHashMap on JRuby). Each value is a
        # Hash { queue:, created_at: }.
        @resp_map = ::Concurrent::Map.new
        @resp_handlers = []
        @cleanup_thread = nil
        @shutdown = false
        @cleanup_mutex = ::Mutex.new
        @cleanup_cv = ::ConditionVariable.new
        @restarting = false # Prevents concurrent restarts
        # Connection the inbox subscription lives on. #start compares this by
        # identity to detect a rebuilt connection (nats-pure fired on_close
        # and made a fresh client) and trigger a restart. An AtomicReference,
        # not a plain ivar, so #start's fast path reads it without LOCK --
        # start runs once per RPC, and LOCK contention on JRuby is real.
        @subscribed_nats = ::Concurrent::AtomicReference.new(nil)

        # Self-healing backoff counter for the dispatcher pool. Atomic so
        # simultaneous crashes don't lose updates. Decays to zero once a
        # dispatcher is healthy (see run_dispatch_loop), so a later crash
        # restarts the backoff from 1s instead of staying at the cap.
        @crash_count = ::Concurrent::AtomicFixnum.new(0)

        # High-water mark of response queue depth since the last cleanup
        # cycle. The cleanup thread emits and resets it
        # (response_muxer.pending_queue_peak), so a burst between gauge
        # samples stays visible.
        @pending_queue_peak = ::Concurrent::AtomicFixnum.new(0)
      end

      def logger
        ::Protobuf::Logging.logger
      end

      # Monotonic clock for token TTL accounting. Ignores wall-clock jumps.
      def monotonic_now
        ::Protobuf::Nats.monotonic_time
      end

      # Number of dispatcher threads draining the response subscription. JRuby
      # has true parallelism, so one dispatcher is a throughput ceiling; fan
      # out to processor_count. CRuby's GVL makes extra dispatchers useless,
      # so stay at 1. Override with an env var.
      #
      # Measured (2026-08-26): throughput peaks at 4 dispatchers and declines
      # above it, from monitor contention (see run_dispatch_loop). Default
      # stays at processor_count: the cost only matters near saturation, far
      # above this deployment's load. Cap with
      # PB_NATS_RESPONSE_MUXER_DISPATCHERS=4 if that changes.
      def dispatcher_count
        @dispatcher_count ||= begin
          default = ::RUBY_ENGINE == "jruby" ? ::Concurrent.processor_count : 1
          ::Protobuf::Nats.env_int("PB_NATS_RESPONSE_MUXER_DISPATCHERS", default, :min => 1)
        end
      end

      # Message-count and byte caps for the response queue (see
      # DEFAULT_RESPONSE_QUEUE_SIZE / _BYTES). #start reads each once, so no
      # memoization is needed.
      def response_queue_size
        ::Protobuf::Nats.env_int("PB_NATS_RESPONSE_MUXER_QUEUE_SIZE", DEFAULT_RESPONSE_QUEUE_SIZE, :min => 1)
      end

      def response_queue_bytes
        ::Protobuf::Nats.env_int("PB_NATS_RESPONSE_MUXER_QUEUE_BYTES", DEFAULT_RESPONSE_QUEUE_BYTES, :min => 1)
      end

      # Current depth of the response queue; 0 before the muxer starts.
      # Matches SuperSubscriptionManager#pending_queue_size.
      def pending_queue_size
        @resp_sub&.pending_queue&.size || 0
      end

      def cleanup(token)
        # Remove and return the entry atomically, then wake and close its queue.
        entry = @resp_map.delete(token)
        wake_and_close_queue(entry[:queue]) if entry
      end

      # Wake any waiter in next_message on this queue, then close it. Pushing
      # QUEUE_WAKE is what wakes a timed pop on JRuby; close alone does not
      # (see QUEUE_WAKE). Safe to call on an already-closed queue.
      def wake_and_close_queue(queue)
        return unless queue
        begin
          queue.push(QUEUE_WAKE)
        rescue ::ClosedQueueError, ::ThreadError
          # Already closed elsewhere; any plain waiter was already woken.
        end
        queue.close
      end

      def next_message(token, timeout)
        # Lock-free read of the per-token queue.
        entry = @resp_map[token]
        queue = entry && entry[:queue]

        unless queue
          logger.warn "Token #{token} not found or already cleaned up during next_message"
          raise ::NATS::Timeout
        end

        if timeout && timeout <= 0
          raise ::NATS::Timeout
        end

        # TimeoutQueue.pop(non_block, timeout:) blocks until a message arrives
        # or the timeout expires (nil), or blocks indefinitely with no timeout.
        begin
          msg = if timeout
                  queue.pop(false, timeout: timeout)
                else
                  queue.pop(false)
                end

          # nil means the queue closed or the timeout expired. QUEUE_WAKE is
          # the sentinel from wake_and_close_queue; treat both as a timeout.
          if msg.nil? || msg.equal?(QUEUE_WAKE)
            logger.warn "Queue closed or timeout for token #{token} during next_message"
            raise ::NATS::Timeout
          end

          msg
        rescue ThreadError
          logger.warn "Queue closed for token #{token} during next_message"
          raise ::NATS::Timeout
        end
      end

      def new_request
        # UUIDv7 encodes creation time. nats.new_inbox's nuid is not thread-safe.
        token = UUIDv7Helper.generate

        # Concurrent::Map#[]= is atomic, so no lock is needed here.
        @resp_map[token] = {
          queue: ::Concurrent::Collection::TimeoutQueue.new,
          created_at: monotonic_now
        }

        ResponseMuxerRequest.new(self, token)
      end

      def publish(subject, data, token)
        unless @resp_inbox_prefix
          raise ::Protobuf::Nats::Errors::ResponseMuxer, "ResponseMuxer not started - cannot publish"
        end

        nats = Protobuf::Nats.client_nats_connection
        # nats-pure drops the memoized connection when it fires on_close after
        # exhausting reconnect attempts. Raise the muxer's retryable error
        # instead of NoMethodError so the client retries and rebuilds it.
        if nats.nil?
          raise ::Protobuf::Nats::Errors::ResponseMuxer, "NATS connection unavailable (closed and not yet rebuilt) - cannot publish"
        end

        # Do not publish while nats-pure reconnects. It buffers the publish
        # (up to 32,768, then blocks this thread with no deadline) and sends
        # the buffer after the reconnect, so a server runs every retry the
        # caller already saw fail: a non-idempotent RPC ran 3 times. jnats
        # had :disable_reconnect_buffer for this; nats-pure has no such
        # option. Raise the retryable error, so the client waits and retries.
        # A status change after this check can still buffer one publish.
        unless nats.connected?
          raise ::Protobuf::Nats::Errors::ResponseMuxer, "NATS connection not connected (status=#{nats.status.inspect}; reconnecting or closed) - cannot publish"
        end

        reply_to = "#{@resp_inbox_prefix}.#{token}"
        nats.publish(subject, data, reply_to)
      end

      def restart
        logger.debug "restarting response_muxer"

        # Only one restart runs at a time.
        LOCK.synchronize do
          if @restarting
            logger.warn "Restart already in progress, skipping concurrent restart request"
            return
          end
          @restarting = true
        end

        # Yield so other restart callers reach the @restarting check above
        # and skip. Without this, CRuby's GVL can let this thread finish the
        # whole restart before a sibling thread enters the method.
        Thread.pass

        begin
          # Stop the existing muxer first, if it is running.
          LOCK.synchronize do
            @resp_handlers.each(&:kill)
            @resp_handlers.clear
            drop_subscription_locked("during restart")
            stop_cleanup_thread
          end

          start
        ensure
          LOCK.synchronize { @restarting = false }
        end
      end

      def start
        current_nats = ::Protobuf::Nats.client_nats_connection

        # Runs once per RPC, so the healthy path stays lock-free: a volatile
        # read of the connection the inbox subscription lives on. Also
        # detects a replaced connection (nats-pure fired on_close and the
        # next request built a fresh client): without a rebuild, every RPC
        # times out on the dead connection until the process restarts.
        subscribed = @subscribed_nats.get
        return if subscribed && (current_nats.nil? || subscribed.equal?(current_nats))

        # Not started, or the connection changed. Re-check under LOCK: the
        # read above can race a concurrent start or restart.
        stale = false
        LOCK.synchronize do
          if _started?
            return if current_nats.nil? || @subscribed_nats.get.equal?(current_nats)
            stale = true
          end
        end

        if stale
          logger.warn "ResponseMuxer NATS connection was replaced; restarting the muxer on the new connection"
          restart
          return
        end

        LOCK.synchronize do
          # Re-check: another thread may have started it while we waited for LOCK.
          return if _started?

          nats = ::Protobuf::Nats.client_nats_connection
          return if nats.nil?

          begin
            @resp_inbox_prefix = nats.new_inbox
            @resp_sub = nats.subscribe("#{@resp_inbox_prefix}.*")

            # run_dispatch_loop uses @resp_sub.synchronize to decrement
            # pending_size after each pop, keeping the byte cap accurate.
            # nats-pure's Subscription always includes MonitorMixin; fail
            # loudly if that ever changes, instead of silently miscounting
            # bytes and dropping every response.
            unless @resp_sub.respond_to?(:synchronize)
              raise ::Protobuf::Nats::Errors::IncompatibleSubscription,
                "NATS subscription does not respond to #synchronize; cannot maintain pending_size byte accounting (nats-pure internals changed?)"
            end

            # Bound the queue by count and bytes (see DEFAULT_RESPONSE_QUEUE_SIZE / _BYTES).
            @resp_sub.pending_msgs_limit = response_queue_size
            @resp_sub.pending_bytes_limit = response_queue_bytes
            @subscribed_nats.set(nats)
            @started = true
          rescue => e
            @resp_inbox_prefix = nil
            @resp_sub = nil
            @subscribed_nats.set(nil)
            @started = false
            logger.error "Failed to start ResponseMuxer: #{e.message}"
            raise
          end
        end

        start_cleanup_thread

        LOCK.synchronize { top_up_dispatchers_locked }
      end

      def started?
        LOCK.synchronize { _started? }
      end

      # True when the muxer's inbox subscription lives on this exact connection
      # object. Uses identity, not equality: a rebuilt connection to the same
      # servers is still a different socket with no subscriptions.
      def subscribed_to?(nats)
        @subscribed_nats.get.equal?(nats)
      end

      # Token TTL. Floors at TOKEN_TTL_SECONDS, but stretches for a longer
      # client response_timeout, so cleanup never closes a queue a caller is
      # still waiting on.
      def token_ttl_seconds
        @token_ttl_seconds ||= [TOKEN_TTL_SECONDS, ::Protobuf::Nats.client_response_timeout + 60].max
      end

      def cleanup_stale_tokens
        cutoff = monotonic_now - token_ttl_seconds

        # Collect stale tokens, then delete. Concurrent::Map iteration holds no
        # global lock, so request threads never block on this O(n) scan.
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
          wake_and_close_queue(data[:queue])
        end

        if stale_count > 0
          ::Protobuf::Nats.instrument "response_muxer.stale_tokens_cleaned", stale_count
        end

        # Gauge the response queue so a growing backlog is visible before it
        # turns into timeouts or SlowConsumer drops. current is depth at
        # sample time; peak is the high-water mark since the last cycle.
        ::Protobuf::Nats.instrument "response_muxer.pending_queue_size", pending_queue_size
        # AtomicFixnum has no get_and_set, so read and reset inside the update block.
        peak = 0
        @pending_queue_peak.update do |current_value|
          peak = current_value
          0 # set to 0
        end
        ::Protobuf::Nats.instrument "response_muxer.pending_queue_peak", peak
      end

      def stop
        LOCK.synchronize do
          stop_cleanup_thread
          @resp_handlers.each(&:kill)
          @resp_handlers.clear
          drop_subscription_locked("during stop")
        end
      end

      private

      def _started?
        !!@started
      end

      # Tear down the inbox subscription and mark the muxer stopped. Caller
      # must hold LOCK. `context` labels the failure log.
      def drop_subscription_locked(context)
        if @resp_sub
          begin
            @resp_sub.unsubscribe
          rescue => e
            logger.warn "Failed to unsubscribe old response muxer subscription #{context}: #{e.message}"
          ensure
            @resp_sub = nil
          end
        end
        @subscribed_nats.set(nil)
        @started = false

        # The inbox prefix dies with the subscription, so no in-flight
        # response can arrive. Closing each token's queue wakes its waiter now
        # (next_message raises NATS::Timeout) instead of blocking until its
        # own timeout; the client's retry path picks it up on the new
        # connection. Entries stay in @resp_map until owner cleanup or the
        # TTL sweep removes them.
        fail_inflight_requests
      end

      # Caller must hold LOCK (only called from drop_subscription_locked).
      def fail_inflight_requests
        @resp_map.each_pair do |_token, entry|
          wake_and_close_queue(entry[:queue])
        end
      end

      # Spawn one dispatcher thread. Dispatchers share @resp_sub.pending_queue
      # (thread-safe Queue) and route messages through the lock-free @resp_map.
      def spawn_dispatcher
        Thread.new do
          Thread.current.name = "response-muxer-#{Thread.current.object_id}"
          begin
            run_dispatch_loop
          rescue => fatal_error
            # Only a fatal error reaches here: ThreadError from the shared
            # pending_queue being closed.
            logger.error("ResponseMuxer thread crashed fatally. Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error)

            # --- Self-healing logic ---
            # Atomic increment so simultaneous crashes don't lose updates.
            # run_dispatch_loop decays this once a dispatcher is healthy, so it
            # only grows under a sustained crash loop.
            crashes = @crash_count.increment
            # Exponential backoff (1, 4, 9, 16s...), capped at 60s.
            sleep_duration = ::Protobuf::Nats.crash_backoff_seconds(crashes)
            logger.warn("Waiting #{sleep_duration}s before attempting to restart ResponseMuxer.")
            sleep sleep_duration
            # --- End of self-healing logic ---

            healed = LOCK.synchronize do
              # Remove this thread before the top-up runs. It is still alive
              # here but about to exit; otherwise select!(&:alive?) would
              # count it as live and spawn no replacement, leaving the pool
              # short (zero dispatchers on CRuby).
              @resp_handlers.delete(::Thread.current)

              # Tear down only the subscription this thread died on. Siblings
              # can crash together on different backoffs; an unconditional
              # teardown could destroy one a sibling already rebuilt and cancel
              # its already-arrived requests via fail_inflight_requests. If a
              # sibling healed us, only the pool top-up is needed.
              #
              # DISPATCHING_SUB_KEY, set by run_dispatch_loop on this thread,
              # names the subscription actually drained -- @resp_sub would
              # give the post-backoff current value instead. nil means this
              # thread died before draining anything.
              dispatching_sub = ::Thread.current[DISPATCHING_SUB_KEY]
              healed = !@resp_sub.nil? && !@resp_sub.equal?(dispatching_sub)
              if healed
                # Not `start`: the muxer is already started on the live
                # connection, so start would return early and skip the top-up.
                logger.info "ResponseMuxer already healed by another dispatcher; replacing this dispatcher without a teardown"
                top_up_dispatchers_locked
              else
                drop_subscription_locked("during self-healing")
              end
              healed
            end
            start unless healed
          end
        end
      end

      # Top up the dispatcher pool to dispatcher_count. Prunes dead threads
      # first so repeated self-healing converges instead of multiplying
      # threads. Caller must hold LOCK.
      def top_up_dispatchers_locked
        @resp_handlers.select!(&:alive?)
        @resp_handlers << spawn_dispatcher while @resp_handlers.size < dispatcher_count
      end

      def run_dispatch_loop
        loop do
          begin
            # --- Start of per-message block ---
            # @resp_sub can briefly be nil during a restart. Park instead of
            # dereferencing nil, which would raise NoMethodError and busy-spin
            # (flooding logs and error callbacks) until it is set.
            sub = @resp_sub
            if sub.nil?
              sleep 0.01
              next
            end

            # Record what this dispatcher drains, for the crash handler in
            # spawn_dispatcher. Thread-local, so each dispatcher tracks its own
            # subscription across restarts.
            ::Thread.current[DISPATCHING_SUB_KEY] = sub

            msg = sub.pending_queue.pop

            # nil means the queue closed (e.g. the connection died). A closed
            # queue returns nil forever, so park briefly instead of spinning
            # at 100% CPU until a restart swaps in a live subscription.
            if msg.nil?
              sleep ::Protobuf::Nats::CLOSED_QUEUE_PARK_SECONDS
              next
            end

            # Drop the popped message's bytes from pending_size. nats-pure
            # only decrements it in #process, bypassed here by popping
            # pending_queue directly; skipping this lets the counter climb and
            # false-trip pending_bytes_limit, dropping every later response.
            # Uses the same monitor nats-pure's read thread holds (#start
            # guarantees #synchronize).
            #
            # Reviewed, left as-is (2026-08-26): this monitor is shared with
            # every subscription on the connection, so dispatchers contend
            # with the read thread for all of them. Measured on JRuby 9.4/15
            # cores: ~9.6us per message; ingress falls to 43% of
            # one-dispatcher rate at 8 dispatchers (203k msg/s), and
            # throughput peaks at 4. This deployment expects <=2000 req/s
            # (~1% of that ceiling), so the cost is noise.
            #
            # Do NOT re-flag this on load alone. Reopen only if peak response
            # rate nears 100k msg/s, or another high-volume subject shares
            # this connection (it then pays this cost even while idle). If it
            # must change: batch the decrement (flush every N messages, ~1.8x
            # at N=8) instead of dropping it, staying well below
            # pending_bytes_limit. Capping PB_NATS_RESPONSE_MUXER_DISPATCHERS
            # at 4 needs no code change.
            sub.synchronize { sub.pending_size -= msg.data.size }

            # Sample post-pop depth into the high-water mark, so a burst that
            # fills and drains between 60s gauge samples stays visible.
            depth = sub.pending_queue.size
            @pending_queue_peak.update { |current_value| [depth, current_value].max }

            dispatch_message(msg)

            # A processed message means this dispatcher is healthy: decay the
            # backoff so a later transient crash restarts it from 1s. Skip the
            # write when already zero, to keep this cheap.
            @crash_count.value = 0 unless @crash_count.value.zero?
            # --- End of per-message block ---
          rescue => per_message_error
            # ThreadError means the queue closed; the loop cannot continue.
            raise if per_message_error.is_a?(::ThreadError)

            # Log and continue; do not kill the thread for one bad message.
            logger.error("ResponseMuxer failed to process a message. Error: #{per_message_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(per_message_error)
          end
        end
      end

      def dispatch_message(msg)
        unless msg.subject.is_a?(String) && msg.subject.include?('.')
          ::Protobuf::Nats.instrument "client.invalid_message", 1

          logger.warn "Received message with invalid subject: #{msg.subject}. Dropping."
          return
        end

        # Subject format: _INBOX.{random_data}.{random_data_msg_id}
        # Hot path: rindex/slice avoids the array and string split() allocates
        # per response. include?('.') above guarantees rindex is non-nil.
        subject = msg.subject
        token = subject[(subject.rindex(".") + 1)..]

        logger.debug { "token: #{token}, resp_map.keys:#{@resp_map.keys}" } if logger.debug?

        # Lock-free read of the per-token queue.
        entry = @resp_map[token]
        queue = entry && entry[:queue]

        unless queue
          # Decode the UUIDv7 timestamp to get the message's age, if possible.
          delay_seconds = UUIDv7Helper.age_in_seconds(token)

          ::Protobuf::Nats.instrument "client.unexpected_message", delay_seconds || 1

          if delay_seconds
            logger.warn "Received unexpected message (#{delay_seconds.round(3)}s old). MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject rescue 'unknown'}. Dropping unexpected message."
          else
            logger.warn "Received unexpected message. MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject rescue 'unknown'}. Dropping unexpected message."
          end
          return
        end

        # Push is lock-free and thread-safe.
        begin
          if queue.size >= MAX_RESPONSES_PER_TOKEN
            logger.warn "Token #{token} has #{queue.size} queued responses. Possible duplicate messages or slow consumer. Dropping message."
            return
          end

          queue.push(msg)
        rescue ThreadError
          # Queue was already closed by cleanup; drop the message.
          logger.debug "Queue closed for token #{token}, dropping message"
        end
      end

      def start_cleanup_thread
        return if @cleanup_thread&.alive?

        @cleanup_mutex.synchronize { @shutdown = false }
        @cleanup_thread = Thread.new do
          begin
            loop do
              # Wait 60 seconds, or until signaled to shut down.
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
        # Named from the outside, so the name is set before start_cleanup_thread
        # returns and cannot race the thread body.
        @cleanup_thread.name = "response-muxer-cleanup-#{object_id}"
      end

      def stop_cleanup_thread
        if @cleanup_thread&.alive?
          @cleanup_mutex.synchronize do
            @shutdown = true
            @cleanup_cv.signal
          end
          @cleanup_thread.join(0.5)
          # Force kill if still alive; should not normally happen.
          @cleanup_thread.kill if @cleanup_thread&.alive?
        end
        @cleanup_thread = nil
      end
    end
  end
end
