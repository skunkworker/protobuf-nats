require "concurrent"

module Protobuf
  module Nats
    class ThreadPool

      def initialize(size, opts = {})
        @queue = ::Queue.new
        # Lock-free counter of in-flight work. Replaces a mutex-guarded integer so
        # that N workers running in true parallel (JRuby) don't serialize on every
        # task completion.
        @active_work = ::Concurrent::AtomicFixnum.new(0)

        # Callbacks
        @error_cb = lambda do |error|
          logger.error("Error in ThreadPool worker: #{error.message}
 #{error.backtrace.join("
")}")
        end

        # Synchronization
        @mutex = ::Mutex.new      # guards the @workers array only
        @cb_mutex = ::Mutex.new

        # Let's get this party started
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

      # Thread-safe access to check if the pool is full.
      def full?
        @active_work.value >= @max_size
      end

      def max_size
        @max_size
      end

      # This method is now thread-safe.
      def push(&work_cb)
        return false if @shutting_down.true?

        # Optimistically claim a slot; back off if we exceeded the cap. This admits
        # work only while active_work < max_size, matching the original guard, but
        # without holding a mutex across the enqueue.
        if @active_work.increment > @max_size
          @active_work.decrement
          return false
        end

        @queue << [:work, work_cb]

        # Supervise outside any lock-held section to avoid holding it during thread creation.
        supervise_workers
        true
      end

      # This method is now thread-safe.
      def shutdown
        # CAS ensures the poison pills are pushed exactly once.
        return unless @shutting_down.make_true

        @max_workers.times { @queue << [:stop, nil] }
      end

      def kill
        @shutting_down.make_true
        @workers.map(&:kill)
      end

      # Wait until all workers exit. Returns true if the pool drained, false if
      # the timeout elapsed first. Prunes under the mutex (it mutates @workers).
      def wait_for_termination(seconds = nil)
        deadline = seconds && (::Protobuf::Nats.monotonic_time + seconds)
        loop do
          @mutex.synchronize { prune_dead_workers }
          return true if @workers.empty?
          return false if deadline && ::Protobuf::Nats.monotonic_time >= deadline
          sleep 0.1
        end
      end

      # Top the pool back up to max_workers if workers have died (e.g. one was
      # killed by a non-StandardError, which the per-task rescue can't catch).
      # No-op while shutting down so we don't resurrect workers mid-drain.
      def replenish
        return if @shutting_down.true?
        supervise_workers
      end

      # This callback is executed in a thread safe manner.
      def on_error(&cb)
        @cb_mutex.synchronize { @error_cb = cb }
      end

      # Thread-safe access to the current active work size.
      def size
        @active_work.value
      end

    private

      def logger
        ::Protobuf::Logging.logger
      end

      def prune_dead_workers
        # This must be called inside @mutex.
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

      def spawn_worker
        ::Thread.new do
          Thread.current.name = "thread-pool-worker"
          loop do
            type, cb = @queue.pop
            begin
              # Break if we're shutting down
              break if type == :stop
              # Perform work
              cb.call
              # Update stats
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
