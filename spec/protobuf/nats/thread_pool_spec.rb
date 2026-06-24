require "spec_helper"

describe ::Protobuf::Nats::ThreadPool do
  describe "#wait_for_termination" do
    it "returns true once the pool drains" do
      pool = described_class.new(2)
      pool.shutdown
      expect(pool.wait_for_termination(2)).to be(true)
    end

    it "returns false when the timeout elapses first" do
      pool = described_class.new(1)
      # No shutdown: workers block on the queue and never exit -> timeout.
      expect(pool.wait_for_termination(0.2)).to be(false)
      pool.kill
    end
  end

  describe "#replenish" do
    it "respawns workers killed outside the per-task rescue" do
      pool = described_class.new(2)
      workers = pool.instance_variable_get(:@workers)
      victim = workers.first
      victim.kill
      victim.join(1)

      pool.replenish

      alive = pool.instance_variable_get(:@workers).select(&:alive?)
      expect(alive.size).to eq(2)
      pool.kill
    end

    it "does not respawn workers once shutting down" do
      pool = described_class.new(2)
      pool.shutdown
      expect(pool.wait_for_termination(2)).to be(true)

      pool.replenish

      expect(pool.instance_variable_get(:@workers).select(&:alive?)).to be_empty
    end
  end
end
