require "concurrent"
require "protobuf/nats/errors"

module Protobuf
  module Nats
    class ThreadPool

      def initialize(size, opts = {})
        @queue = ::Queue.new
        # Lock-free counter of in-flight work, so parallel workers on JRuby
        # don't serialize on every task completion.
        @active_work = ::Concurrent::AtomicFixnum.new(0)

        @error_cb = lambda do |error|
          logger.error("Error in ThreadPool worker: #{error.message}
 #{error.backtrace.join("
")}")
        end

        @mutex = ::Mutex.new      # guards the @workers array only
        @cb_mutex = ::Mutex.new

        queue_size = opts[:max_queue].to_i || 0
        @max_size = size + queue_size
        @max_workers = size
        @shutting_down = ::Concurrent::AtomicBoolean.new(false)
        @workers = []
        supervise_workers
      end

      def enqueued_size
        @queue.size
      end

      def full?
        @active_work.value >= @max_size
      end

      def max_size
        @max_size
      end

      def push(&work_cb)
        return false if @shutting_down.true?

        # Claim a slot first, then back off over the cap. Admits work while
        # `active_work < max_size`, without a mutex across the enqueue.
        if @active_work.increment > @max_size
          @active_work.decrement
          return false
        end

        @queue << [:work, work_cb]
        true
      end

      def shutdown
        # CAS ensures the poison pills push exactly once.
        return unless @shutting_down.make_true

        @max_workers.times { @queue << [:stop, nil] }
      end

      def kill
        @shutting_down.make_true
        @workers.map(&:kill)
      end

      # Wait until all workers exit. Returns true if drained, false on
      # timeout. Prune under the mutex, since it changes `@workers`.
      def wait_for_termination(seconds = nil)
        deadline = seconds && (::Protobuf::Nats.monotonic_time + seconds)
        loop do
          @mutex.synchronize { prune_dead_workers }
          if @workers.empty?
            # A push past the `@shutting_down` check can land after the last
            # worker drains and exits, so nothing would run it though the
            # server already ACKed it. Run it here on the caller's thread.
            # This ignores `seconds`: a slow handler can hold the caller past
            # the deadline. Accepted, since the alternative drops an ACKed
            # request, and only a few pushes can land here.
            drain_remaining_work(requeue_pills: false)
            return true
          end
          return false if deadline && ::Protobuf::Nats.monotonic_time >= deadline
          sleep 0.1
        end
      end

      # Replace workers killed by a non-`StandardError` past the per-task
      # rescue. The only respawn path after `initialize`: `#push` skips
      # supervising, since a mutex plus an `alive?` scan would add
      # contention to the hot enqueue path. The server's run loop calls this
      # every second instead. No-op while shutting down, to not restart
      # workers mid-drain.
      def replenish
        return if @shutting_down.true?
        supervise_workers
      end

      def on_error(&cb)
        @cb_mutex.synchronize { @error_cb = cb }
      end

      def size
        @active_work.value
      end

    private

      def logger
        ::Protobuf::Logging.logger
      end

      # Call this only inside `@mutex`.
      def prune_dead_workers
        @workers = @workers.select(&:alive?)
      end

      def supervise_workers
        @mutex.synchronize do
          prune_dead_workers
          missing_worker_count = (@max_workers - @workers.size)
          missing_worker_count.times do
            @workers << spawn_worker
          end
        end
      end

      # Run any `:work` left behind a poison pill, then stop. A worker calls
      # this after taking its pill; `#wait_for_termination` also calls it
      # once more after the last worker exits (see there for why).
      #
      # This does not make admission and shutdown atomic: `#push` is
      # lock-free, so work can still arrive after the final drain. It closes
      # the window that matters instead, running everything enqueued up to
      # the moment the pool reports termination, so no ACKed request drops.
      #
      # `requeue_pills`: true when a worker drains, since a pill found there
      # belongs to a live sibling. False for the final drain: no worker is
      # left, so any pill is an orphan (e.g. one for a worker that died and
      # was not replaced during shutdown); discard it.
      def drain_remaining_work(requeue_pills: true)
        loop do
          begin
            type, cb = @queue.pop(true) # non_block: empty queue ends the drain
          rescue ::ThreadError
            break
          end

          # A sibling's pill: put it back so that worker still exits, then
          # stop draining. An orphan pill (see `requeue_pills`): discard it.
          if type == :stop
            next unless requeue_pills
            @queue << [:stop, nil]
            break
          end

          begin
            cb.call
          rescue => error
            @cb_mutex.synchronize { @error_cb.call(error) }
          ensure
            @active_work.decrement
          end
        end
      end

      # The opt-in overdue reclaim (server feature) raises HandlerOverdue
      # into a worker with Thread#raise, which can land at any point. Landed
      # in the ensure below, it skipped the decrement and leaked a pool slot
      # for good (the pool then NACKed every request once full); landed
      # between tasks, outside the per-task rescue, it killed the worker.
      # So defer it everywhere, and accept it only while the task runs.
      OVERDUE_DEFERRED = { ::Protobuf::Nats::Errors::HandlerOverdue => :never }.freeze
      OVERDUE_ACCEPTED = { ::Protobuf::Nats::Errors::HandlerOverdue => :immediate }.freeze

      def spawn_worker
        # A new thread takes the creating thread's interrupt mask, so create
        # it inside the mask. A mask set in the thread body comes too late
        # on CRuby: a raise can land before the body runs.
        ::Thread.handle_interrupt(OVERDUE_DEFERRED) { ::Thread.new { run_worker } }
      end

      def run_worker
        Thread.current.name = "thread-pool-worker"
        ::Thread.handle_interrupt(OVERDUE_DEFERRED) do
          loop do
            type, cb = @queue.pop

            # The `:stop` pill never claimed an `@active_work` slot (see
            # `#shutdown`), so it must skip the ensure below: decrementing
            # for it would drive the counter negative.
            if type == :stop
              # `#shutdown` can slip pills in between `#push`'s
              # `@shutting_down` check and its enqueue, stranding real work
              # behind them otherwise. The server already ACKed that work,
              # so its client would block until `response_timeout` (60s).
              # See `#drain_remaining_work`.
              drain_remaining_work
              break
            end

            begin
              # A reclaim raise deferred since the last task was for that
              # task, which is done. Discard it before this task starts.
              begin
                ::Thread.handle_interrupt(OVERDUE_ACCEPTED) {}
              rescue ::Protobuf::Nats::Errors::HandlerOverdue
                nil
              end
              ::Thread.handle_interrupt(OVERDUE_ACCEPTED) { cb.call }
            rescue => error
              @cb_mutex.synchronize { @error_cb.call(error) }
            ensure
              @active_work.decrement
            end
          end
        end
      end

    end
  end
end
