require "concurrent"

module Protobuf
  module Nats
    # A `SizedQueue` that also bounds total bytes, not just message count.
    # One shared intake queue serves all subscriptions, so a per-subscription
    # limit (nats-pure's `pending_bytes_limit`) can't bound the combined
    # heap; this shared counter can.
    #
    # A push over the byte limit drops the message instead of blocking, like
    # nats-pure's SlowConsumer: pushes run on nats-pure's read thread
    # (`Subscription#dispatch`), and blocking it would stall PING/PONG. The
    # `:shutdown` poison pill counts as zero bytes and is never dropped.
    #
    # A drop calls the optional `on_drop` callback with the byte count, so
    # the caller owns instrumentation.
    class ByteBoundedQueue < ::SizedQueue
      def initialize(max_msgs, max_bytes, on_drop: nil)
        super(max_msgs)
        @max_bytes = max_bytes
        @on_drop = on_drop
        @bytes = ::Concurrent::AtomicFixnum.new(0)
      end

      # Enqueue unless it would exceed the byte limit. The check-then-add
      # races only concurrent pops, so this soft limit (like nats-pure's own
      # byte accounting) can be exceeded by at most one in-flight message.
      # Returns `self`, the `SizedQueue#push` contract.
      def push(obj, non_block = false)
        bytes = byte_size(obj)
        if bytes > 0 && (@bytes.value + bytes) > @max_bytes
          @on_drop&.call(bytes)
          return self
        end

        # Count bytes before the enqueue, and roll back on failure. Counting
        # after `super` would let a pop subtract first; `#pop`'s zero-clamp
        # then swallows it, and the later increment permanently overcounts a
        # message already gone. That drift only climbs, jamming every push
        # once it hits `@max_bytes` -- same bug class as nats-pure's
        # `pending_size` bug.
        #
        # A blocking push on a full queue counts bytes while it waits: a
        # deliberate short overcount, resolved once the push completes.
        @bytes.increment(bytes) if bytes > 0
        pushed = false
        begin
          super(obj, non_block)
          pushed = true
        ensure
          # `ensure`, not `rescue`: also rolls back on an async
          # `Thread#raise` or non-`StandardError` unwind.
          @bytes.decrement(bytes) if bytes > 0 && !pushed
        end
        self
      end
      alias_method :<<, :push

      def pop(non_block = false)
        obj = super
        # nil means closed or empty (non-blocking): nothing to subtract. The
        # zero-clamp only guards `#clear` racing an in-flight pop; `#push`
        # counts bytes first, so a normal pop always finds them present.
        @bytes.update { |value| [value - byte_size(obj), 0].max } if obj
        obj
      end

      def clear
        super
        @bytes.value = 0
      end

      # Current resident byte total (an observability gauge).
      def bytesize
        @bytes.value
      end

      private

      # Bytes charged to a queued item. `NATS::Msg` carries `#data`; the
      # `:shutdown` poison pill and other sentinels count as 0.
      def byte_size(obj)
        obj.respond_to?(:data) && obj.data ? obj.data.bytesize : 0
      end
    end
  end
end
