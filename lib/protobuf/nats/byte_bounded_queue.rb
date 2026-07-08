require "concurrent"

module Protobuf
  module Nats
    # A SizedQueue that additionally bounds the total *bytes* of its contents,
    # not just the message count. The server funnels every subscription into one
    # shared intake queue, so a per-subscription byte limit (nats-pure's
    # pending_bytes_limit) can't bound the aggregate heap -- this shared counter
    # can. Count is still bounded by the SizedQueue capacity it inherits.
    #
    # When a push would exceed the byte ceiling we DROP the message rather than
    # block: pushes happen on nats-pure's read thread (Subscription#dispatch), and
    # blocking it would stall PING/PONG and every other subject. A drop mirrors
    # nats-pure's own SlowConsumer behaviour. Non-message items (the :shutdown
    # poison pill) carry zero bytes, so they are never dropped by the byte gate.
    #
    # A drop invokes the optional +on_drop+ callback with the dropped byte count,
    # so the caller owns any (context-specific) instrumentation rather than this
    # generic queue class hard-coding it.
    class ByteBoundedQueue < ::SizedQueue
      def initialize(max_msgs, max_bytes, on_drop: nil)
        super(max_msgs)
        @max_bytes = max_bytes
        @on_drop = on_drop
        @bytes = ::Concurrent::AtomicFixnum.new(0)
      end

      # Enqueue unless it would exceed the byte ceiling. The check-then-add races
      # only concurrent pops (which lower @bytes), so the ceiling can be exceeded
      # by at most one in-flight message -- a soft limit, like nats-pure's own
      # byte accounting. Returns self (SizedQueue#push contract). Raises
      # ThreadError from super on a non_block push into a count-full queue, before
      # any bytes are counted.
      def push(obj, non_block = false)
        bytes = byte_size(obj)
        if bytes > 0 && (@bytes.value + bytes) > @max_bytes
          @on_drop&.call(bytes)
          return self
        end
        super(obj, non_block)
        @bytes.increment(bytes)
        self
      end
      alias_method :<<, :push

      def pop(non_block = false)
        obj = super
        # nil == closed/empty non_block; nothing dequeued, nothing to subtract.
        @bytes.update { |value| [value - byte_size(obj), 0].max } if obj
        obj
      end

      def clear
        super
        @bytes.value = 0
      end

      # Current resident byte total (gauge for observability).
      def bytesize
        @bytes.value
      end

      private

      # Bytes attributable to a queued item. NATS::Msg carries #data; the
      # :shutdown poison pill (and any other non-message sentinel) counts as 0.
      def byte_size(obj)
        obj.respond_to?(:data) && obj.data ? obj.data.bytesize : 0
      end
    end
  end
end
