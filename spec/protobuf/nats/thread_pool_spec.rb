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

  describe "#shutdown" do
    it "does not drive the active-work counter negative (the :stop pill never claimed a slot)" do
      pool = described_class.new(2)
      pool.shutdown
      expect(pool.wait_for_termination(2)).to be(true)
      expect(pool.size).to eq(0)
    end
  end

  describe "overdue-reclaim raise between tasks" do
    it "survives a HandlerOverdue raised while parked on the queue" do
      pool = described_class.new(1)
      worker = pool.instance_variable_get(:@workers).first

      # Wait until the worker is actually parked in @queue.pop (inside the
      # rescue); a raise during thread startup lands outside it, which is the
      # (acceptable) replenish-covered case, not the one under test.
      wait_until(timeout: 2, interval: 0.01) { worker.status == "sleep" }

      worker.raise(::Protobuf::Nats::Errors::HandlerOverdue, "late reclaim")
      sleep 0.1

      expect(worker.alive?).to be(true)
      # The worker still processes work afterwards.
      done = ::Queue.new
      pool.push { done << :ok }
      expect(done.pop).to eq(:ok)
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

    it "is the only respawn path: push does not supervise the worker pool (hot-path contention)" do
      pool = described_class.new(2)
      workers = pool.instance_variable_get(:@workers)
      victim = workers.first
      victim.kill
      victim.join(1)

      expect(pool.push { :noop }).to eq(true)

      # push enqueued the work but must not have replaced the dead worker;
      # only the periodic replenish (server run loop) does that.
      alive = pool.instance_variable_get(:@workers).select(&:alive?)
      expect(alive.size).to eq(1)

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
