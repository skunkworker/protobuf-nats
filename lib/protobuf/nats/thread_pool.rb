module Protobuf
  module Nats
    class ThreadPool

      def initialize(size, opts = {})
        @queue = ::Queue.new
        @active_work = 0

        # Callbacks
        @error_cb = lambda do |error|
          logger.error("Error in ThreadPool worker: #{error.message}
 #{error.backtrace.join("
")}")
        end

        # Synchronization
        @mutex = ::Mutex.new
        @cb_mutex = ::Mutex.new

        # Let's get this party started
        queue_size = opts[:max_queue].to_i || 0
        @max_size = size + queue_size
        @max_workers = size
        @shutting_down = false
        @workers = []
        supervise_workers
      end

      def enqueued_size
        @queue.size
      end

      # Thread-safe access to check if the pool is full.
      def full?
        @mutex.synchronize { @active_work >= @max_size }
      end

      def max_size
        @max_size
      end

      # This method is now thread-safe.
      def push(&work_cb)
        @mutex.synchronize do
          # Re-check conditions inside the lock to guarantee safety.
          return false if @active_work >= @max_size
          return false if @shutting_down

          @queue << [:work, work_cb]
          @active_work += 1
        end

        # Supervise outside the lock to avoid holding it during thread creation.
        supervise_workers
        true
      end

      # This method is now thread-safe.
      def shutdown
        @mutex.synchronize do
          return if @shutting_down # Prevent sending stop messages multiple times
          @shutting_down = true
        end

        # Pushing poison pills can happen outside the lock.
        @max_workers.times { @queue << [:stop, nil] }
      end

      def kill
        @shutting_down = true
        @workers.map(&:kill)
      end

      def wait_for_termination(seconds = nil)
        started_at = ::Time.now
        loop do
          sleep 0.1
          break if seconds && (::Time.now - started_at) >= seconds
          break if @workers.empty?
          prune_dead_workers
        end
      end

      # This callback is executed in a thread safe manner.
      def on_error(&cb)
        @cb_mutex.synchronize { @error_cb = cb }
      end

      # Thread-safe access to the current active work size.
      def size
        @mutex.synchronize { @active_work }
      end

    private

      def logger
        ::Protobuf::Logging.logger
      end

      def prune_dead_workers
        # This must be called inside a mutex block.
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
              @mutex.synchronize { @active_work -= 1 }
            end
          end
        end
      end

    end
  end
end
