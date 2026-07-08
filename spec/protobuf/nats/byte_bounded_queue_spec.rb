require "spec_helper"

describe ::Protobuf::Nats::ByteBoundedQueue do
  def msg(bytes)
    ::NATS::Msg.new(:subject => "x", :data => "x" * bytes)
  end

  # Build a queue whose on_drop callback records the byte count of each drop.
  def queue_with_drops(max_msgs, max_bytes)
    dropped = []
    q = described_class.new(max_msgs, max_bytes, :on_drop => lambda { |bytes| dropped << bytes })
    [q, dropped]
  end

  describe "byte accounting" do
    it "tracks resident bytes as items are pushed and popped" do
      q = described_class.new(100, 10_000)
      q.push(msg(500))
      q << msg(300)

      expect(q.size).to eq(2)
      expect(q.bytesize).to eq(800)

      q.pop
      expect(q.bytesize).to eq(300)
    end

    it "resets the byte counter on clear" do
      q = described_class.new(100, 10_000)
      q.push(msg(500))
      q.clear
      expect(q.size).to eq(0)
      expect(q.bytesize).to eq(0)
    end

    it "counts non-message sentinels (the :shutdown poison pill) as zero bytes" do
      q = described_class.new(100, 10_000)
      q.push(:shutdown)
      expect(q.size).to eq(1)
      expect(q.bytesize).to eq(0)
    end
  end

  describe "byte ceiling (negative paths)" do
    it "drops a message that would exceed the byte ceiling and reports it via on_drop" do
      q, dropped = queue_with_drops(100, 10) # 10-byte ceiling
      q.push(msg(6)) # ok: 6 <= 10
      q.push(msg(6)) # 6 + 6 = 12 > 10 -> drop

      expect(q.size).to eq(1)          # the second message was not enqueued
      expect(q.bytesize).to eq(6)      # ...and its bytes were not counted
      expect(dropped).to eq([6])       # ...and the drop was reported
    end

    it "does not require an on_drop callback" do
      q = described_class.new(100, 10) # no on_drop
      expect { q.push(msg(64)) }.not_to raise_error
      expect(q.size).to eq(0)
    end

    it "still accepts messages after a pop frees byte headroom" do
      q = described_class.new(100, 10)
      q.push(msg(8))
      q.push(msg(8)) # dropped: 16 > 10
      expect(q.size).to eq(1)

      q.pop          # frees 8 bytes -> bytesize 0
      q.push(msg(8)) # now fits
      expect(q.size).to eq(1)
      expect(q.bytesize).to eq(8)
    end

    it "never drops a zero-byte sentinel even when the byte ceiling is already reached" do
      q = described_class.new(100, 5)
      q.push(msg(5)) # at the ceiling
      q.push(:shutdown)
      expect(q.size).to eq(2)
      expect(q.bytesize).to eq(5)
    end

    it "drops a single message larger than the entire byte ceiling, even into an empty queue" do
      q, dropped = queue_with_drops(100, 10)

      q.push(msg(64)) # 64 > 10, queue empty

      expect(q.size).to eq(0)
      expect(q.bytesize).to eq(0)
      expect(dropped).to eq([64])
    end

    it "admits a message that lands exactly on the byte ceiling (boundary is inclusive)" do
      q = described_class.new(100, 10)
      q.push(msg(10)) # 10 == 10, not > 10
      expect(q.size).to eq(1)
      expect(q.bytesize).to eq(10)
    end
  end

  describe "message-count ceiling (inherited SizedQueue)" do
    it "raises ThreadError on a non-blocking push into a count-full queue without counting bytes" do
      q = described_class.new(2, 10_000) # 2-message capacity
      q.push(msg(100))
      q.push(msg(100))
      expect(q.bytesize).to eq(200)

      expect { q.push(msg(100), true) }.to raise_error(::ThreadError)
      expect(q.size).to eq(2)
      expect(q.bytesize).to eq(200) # dropped push did not add bytes
    end

    it "raises ThreadError on a non-blocking pop from an empty queue without underflowing the byte counter" do
      q = described_class.new(2, 10_000)
      expect { q.pop(true) }.to raise_error(::ThreadError)
      expect(q.bytesize).to eq(0)
    end
  end
end
