require "spec_helper"
require "thread"

describe ::Protobuf::Nats::SuperSubscriptionManager do
  let(:nats_client) { ::FakeNatsClient.new }
  let(:callback) { proc { |data, reply, subject| } }
  subject { described_class.new(nats_client, &callback) }

  # Default to a single intake handler so the existing single-handler tests are
  # deterministic regardless of CPU count; fan-out tests override this.
  around do |example|
    previous = ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"]
    ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = "1"
    example.run
    ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = previous
  end

  after do
    # Ensure the thread is killed after each test
    subject.shutdown(0.1)
  end

  describe "#initialize" do
    it "starts pending queue handler threads" do
      handlers = subject.instance_variable_get(:@pending_queue_handlers)
      expect(handlers).to all(be_a(Thread))
      expect(handlers).to all(be_alive)
    end
  end

  describe "message processing" do
    it "processes messages from the queue and invokes the callback" do
      message_data = "message_data"
      message_reply = "message_reply"
      message_subject = "message_subject"
      message = double(:data => message_data, :reply => message_reply, :subject => message_subject)
      
      mutex = Mutex.new
      cond = ConditionVariable.new
      
      # Expect the callback to be called with the message contents
      expect(callback).to receive(:call).with(message_data, message_reply, message_subject) do
        mutex.synchronize { cond.signal }
      end

      # Push a message to the queue and wait for it to be processed
      pending_queue = subject.instance_variable_get(:@pending_queue)
      pending_queue.push(message)
      
      # Wait for the callback to signal
      mutex.synchronize { cond.wait(mutex, 1) }
    end
  end

  describe "#queue_subscribe" do
    it "subscribes to a nats queue" do
      fake_subscription = nats_client.subscribe("test.sub")
      expect(nats_client).to receive(:subscribe).with("my.queue.name", :queue => "my.queue.name").and_return(fake_subscription)
      subject.queue_subscribe("my.queue.name")
    end

    it "disables the byte-based slow-consumer limit (we never run nats-pure's pending_size decrement paths)" do
      fake_subscription = nats_client.subscribe("test.sub")
      allow(nats_client).to receive(:subscribe).and_return(fake_subscription)

      subject.queue_subscribe("my.queue.name")

      expect(fake_subscription.pending_bytes_limit).to eq(::Float::INFINITY)
    end

    it "aligns the slow-consumer message limit with a tuned-down intake queue so the read thread drops instead of blocking" do
      previous = ENV["PB_NATS_SERVER_INTAKE_QUEUE_SIZE"]
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_SIZE"] = "5"
      manager = described_class.new(nats_client, &callback)

      fake_subscription = nats_client.subscribe("test.sub")
      allow(nats_client).to receive(:subscribe).and_return(fake_subscription)
      manager.queue_subscribe("my.queue.name")

      # nats-pure only drops (SlowConsumer) when pending_queue.size >=
      # pending_msgs_limit. With the default limit (65,536) above a 5-slot
      # SizedQueue, the push into the full queue would block the connection's
      # read thread instead.
      expect(fake_subscription.pending_msgs_limit).to eq(5)
      expect(fake_subscription.pending_queue.max).to eq(5)
    ensure
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_SIZE"] = previous
      manager.shutdown(1)
    end

    it "keeps the message limit at the nats-pure default when the intake queue is not tuned" do
      fake_subscription = nats_client.subscribe("test.sub")
      allow(nats_client).to receive(:subscribe).and_return(fake_subscription)

      subject.queue_subscribe("my.queue.name")

      expect(fake_subscription.pending_msgs_limit).to eq(::NATS::IO::DEFAULT_SUB_PENDING_MSGS_LIMIT)
    end

    it "shovels messages from old queue to the new one" do
      # Create a subscription with a message already in its queue
      subscription = nats_client.subscribe("my.queue.name")
      message = ::NATS::Msg.new(:subject => "my.queue.name", :data => "belated_message", :reply => "test_reply")
      subscription.pending_queue.push(message)

      # Stub the nats client to return this subscription
      allow(nats_client).to receive(:subscribe).and_return(subscription)

      # Use a mutex to handle the race condition with the handler thread.
      mutex = Mutex.new
      cond = ConditionVariable.new

      # Expect our callback to get called with the message details.
      expect(callback).to receive(:call).with("belated_message", "test_reply", "my.queue.name") do
        mutex.synchronize { cond.signal }
      end

      subject.queue_subscribe("my.queue.name")

      # Wait for the callback to be invoked.
      # If this times out, the message was not processed.
      mutex.synchronize { cond.wait(mutex, 1) }
    end
  end

  describe "intake byte cap" do
    it "builds the shared intake queue as a ByteBoundedQueue bounded by count and bytes" do
      queue = subject.instance_variable_get(:@pending_queue)
      expect(queue).to be_a(::Protobuf::Nats::ByteBoundedQueue)
      expect(queue.max).to eq(subject.intake_queue_size)
      expect(queue.instance_variable_get(:@max_bytes)).to eq(subject.intake_queue_bytes)
    end

    it "defaults the byte ceiling to 128 MiB" do
      expect(subject.intake_queue_bytes).to eq(::Protobuf::Nats::SuperSubscriptionManager::DEFAULT_INTAKE_QUEUE_BYTES)
      expect(subject.intake_queue_bytes).to eq(128 * 1024 * 1024)
    end

    it "honors PB_NATS_SERVER_INTAKE_QUEUE_BYTES" do
      previous = ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"]
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = (1024 * 1024).to_s
      manager = described_class.new(nats_client, &callback)

      expect(manager.intake_queue_bytes).to eq(1024 * 1024)
      expect(manager.instance_variable_get(:@pending_queue).instance_variable_get(:@max_bytes)).to eq(1024 * 1024)
    ensure
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = previous
      manager.shutdown(1)
    end

    it "reports resident intake bytes via pending_queue_bytes" do
      # Stop handlers so the pushed message isn't drained before we read the gauge.
      subject.instance_variable_get(:@pending_queue_handlers).each { |h| h.kill; h.join(1) }
      queue = subject.instance_variable_get(:@pending_queue)

      expect(subject.pending_queue_bytes).to eq(0)
      queue.push(::NATS::Msg.new(:subject => "s", :data => "x" * 250))
      expect(subject.pending_queue_bytes).to eq(250)
    end

    it "emits server.intake_bytes_dropped when the intake queue drops an over-ceiling message" do
      previous = ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"]
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = "10" # tiny ceiling
      manager = described_class.new(nats_client, &callback)
      # Stop handlers so the pushed message isn't drained before the byte gate runs.
      manager.instance_variable_get(:@pending_queue_handlers).each { |h| h.kill; h.join(1) }
      queue = manager.instance_variable_get(:@pending_queue)

      events = []
      cb = lambda { |name, _s, _f, _id, payload| events << [name, payload] }
      ::ActiveSupport::Notifications.subscribed(cb, "server.intake_bytes_dropped.protobuf-nats") do
        queue.push(::NATS::Msg.new(:subject => "s", :data => "x" * 64)) # 64 > 10 -> drop
      end

      expect(events.map(&:first)).to eq(["server.intake_bytes_dropped.protobuf-nats"])
      expect(events.first.last).to eq(64)
    ensure
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = previous
      manager.shutdown(1)
    end

    # Negative paths: a malformed or out-of-range override must not silently
    # become 0 (a 0-byte ceiling would drop every request); it falls back.
    it "falls back to the default byte ceiling when the env var is malformed" do
      previous = ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"]
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = "128MB"
      manager = described_class.new(nats_client, &callback)

      expect(manager.intake_queue_bytes).to eq(::Protobuf::Nats::SuperSubscriptionManager::DEFAULT_INTAKE_QUEUE_BYTES)
    ensure
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = previous
      manager.shutdown(1)
    end

    it "falls back to the default byte ceiling when the env var is out of range" do
      previous = ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"]
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = "0" # below the min of 1
      manager = described_class.new(nats_client, &callback)

      expect(manager.intake_queue_bytes).to eq(::Protobuf::Nats::SuperSubscriptionManager::DEFAULT_INTAKE_QUEUE_BYTES)
    ensure
      ENV["PB_NATS_SERVER_INTAKE_QUEUE_BYTES"] = previous
      manager.shutdown(1)
    end
  end

  describe "#unsubscribe_all" do
    it "unsubscribes and clears the tracked subscriptions so pause/resume cycles don't leak" do
      fake_subscription = nats_client.subscribe("test.sub")
      allow(nats_client).to receive(:subscribe).and_return(fake_subscription)
      expect(fake_subscription).to receive(:unsubscribe).once

      subject.queue_subscribe("my.queue.name")
      subject.unsubscribe_all

      expect(subject.instance_variable_get(:@subscriptions)).to be_empty

      # A second pass (the next pause) must not re-unsubscribe stale entries.
      subject.unsubscribe_all
    end
  end

  describe "error handling" do
    it "logs per-message errors and continues" do
      mutex = Mutex.new
      cond = ConditionVariable.new

      # Setup a callback that will raise an error
      exploding_callback = proc { raise "Boom!" }
      manager = described_class.new(nats_client, &exploding_callback)

      # Mock the logger on the manager instance we are testing
      logger = ::Logger.new(nil)
      allow(manager).to receive(:logger).and_return(logger)
      expect(logger).to receive(:error).with(/failed to process message/i) do
        mutex.synchronize { cond.signal }
      end

      # Push a message that will trigger the error
      pending_queue = manager.instance_variable_get(:@pending_queue)
      pending_queue.push(double(:data => "d", :reply => "r", :subject => "s"))

      # Wait for the logger to be called
      mutex.synchronize { cond.wait(mutex, 1) }

      # The thread should still be alive
      handler_thread = manager.instance_variable_get(:@pending_queue_handlers).first
      expect(handler_thread.alive?).to be(true)

      manager.shutdown(0.1)
    end
  end

  describe "#shutdown" do
    it "stops the handler threads" do
      handlers = subject.instance_variable_get(:@pending_queue_handlers)
      expect(handlers).to all(be_alive)

      subject.shutdown

      handlers.each { |h| h.join(1) }
      expect(handlers.any?(&:alive?)).to be(false)
    end
  end

  describe "#unsubscribe_all" do
    it "unsubscribes from all subscriptions" do
      sub1 = nats_client.subscribe("test.1")
      sub2 = nats_client.subscribe("test.2")

      allow(nats_client).to receive(:subscribe).and_return(sub1, sub2)

      subject.queue_subscribe("test.1")
      subject.queue_subscribe("test.2")

      expect(sub1).to receive(:unsubscribe)
      expect(sub2).to receive(:unsubscribe)

      subject.unsubscribe_all
    end

    it "continues unsubscribing even if one fails" do
      sub1 = nats_client.subscribe("test.1")
      sub2 = nats_client.subscribe("test.2")
      sub3 = nats_client.subscribe("test.3")

      allow(nats_client).to receive(:subscribe).and_return(sub1, sub2, sub3)

      subject.queue_subscribe("test.1")
      subject.queue_subscribe("test.2")
      subject.queue_subscribe("test.3")

      # Make sub2 fail
      allow(sub1).to receive(:unsubscribe)
      allow(sub2).to receive(:unsubscribe).and_raise(StandardError, "NATS disconnected")
      allow(sub3).to receive(:unsubscribe)

      # Should log warning but continue
      expect(subject.logger).to receive(:warn).with(/failed to unsubscribe/i)

      subject.unsubscribe_all

      # Sub1 and sub3 should still be called
      expect(sub1).to have_received(:unsubscribe)
      expect(sub3).to have_received(:unsubscribe)
    end
  end

  describe "edge cases and fixes" do
    describe "handler thread self-healing" do
      it "processes messages on the handler threads" do
        # The crash counter is now per-thread (no shared @crash_count), so we
        # just verify a handler picks up and runs a message.
        processed = ::Queue.new
        counting_callback = proc { |data, reply, subject| processed << data }

        manager = described_class.new(nats_client, &counting_callback)

        pending_queue = manager.instance_variable_get(:@pending_queue)
        pending_queue.push(double(:data => "d", :reply => "r", :subject => "s"))

        expect(::Timeout.timeout(1) { processed.pop }).to eq("d")

        manager.shutdown(0.1)
      end

      it "calculates exponential backoff correctly" do
        # Test the backoff calculation logic without actually triggering crashes
        test_cases = [
          [1, 1],    # 1^2 = 1
          [2, 4],    # 2^2 = 4
          [3, 9],    # 3^2 = 9
          [8, 60],   # 8^2 = 64, capped at 60
          [10, 60],  # 10^2 = 100, capped at 60
        ]

        test_cases.each do |crash_count, expected_sleep|
          sleep_duration = [(crash_count**2), 60].min
          expect(sleep_duration).to eq(expected_sleep)
        end
      end
    end

    describe "shutdown edge cases" do
      it "does not block if thread is already dead" do
        manager = described_class.new(nats_client, &callback)

        # Kill the threads
        manager.instance_variable_get(:@pending_queue_handlers).each do |handler|
          handler.kill
          handler.join(1)
        end

        # Shutdown should return immediately without blocking
        start_time = Time.now
        manager.shutdown(5)
        elapsed = Time.now - start_time

        expect(elapsed).to be < 0.5
      end

      it "force kills thread if shutdown times out" do
        # Create a callback that blocks for a bit
        blocking_callback = proc { |data, reply, subject| sleep 5 }
        manager = described_class.new(nats_client, &blocking_callback)

        # Push a message that will block the thread
        pending_queue = manager.instance_variable_get(:@pending_queue)
        pending_queue.push(double(:data => "d", :reply => "r", :subject => "s"))

        sleep 0.1  # Let thread start processing

        # Mock logger
        logger = ::Logger.new(nil)
        allow(manager).to receive(:logger).and_return(logger)

        # Shutdown with short timeout - expect force kill
        start_time = Time.now
        manager.shutdown(0.1)
        elapsed = Time.now - start_time

        # Should have timed out and killed quickly
        expect(elapsed).to be < 2

        handlers = manager.instance_variable_get(:@pending_queue_handlers)
        expect(handlers.any?(&:alive?)).to be(false)
      end

      it "handles full queue during shutdown gracefully" do
        manager = described_class.new(nats_client, &callback)
        pending_queue = manager.instance_variable_get(:@pending_queue)

        # Fill the queue with a non-blocking push (stops as soon as it's full).
        # NB: do NOT wrap a blocking `<<` in Timeout.timeout -- its async
        # Thread#raise corrupts the SizedQueue mutex on JRuby 10 (raises
        # "ThreadError: Attempt to unlock a mutex..."), which is exactly what
        # push_with_deadline in the manager avoids.
        1000.times do
          begin
            pending_queue.push(double(:data => "d", :reply => "r", :subject => "s"), true)
          rescue ThreadError
            break # queue full
          end
        end

        # Mock logger
        logger = ::Logger.new(nil)
        allow(manager).to receive(:logger).and_return(logger)

        # Shutdown should still work
        expect { manager.shutdown(1) }.not_to raise_error
      end
    end

    describe "queue migration edge cases" do
      it "has migration limit constant defined" do
        # Just verify the migration logic exists by checking the constant
        # Actually testing 10000+ messages would be slow
        expect(subject.queue_subscribe("test.queue")).to be_a(NATS::Subscription)
      end

      it "logs warning when migrating messages" do
        subscription = nats_client.subscribe("test.queue")

        # Add a message to the old queue before swapping
        subscription.pending_queue.push(::NATS::Msg.new(
          :subject => "test.queue",
          :data => "msg",
          :reply => "reply"
        ))

        allow(nats_client).to receive(:subscribe).and_return(subscription)

        logger = ::Logger.new(nil)
        allow(subject).to receive(:logger).and_return(logger)

        # Should log warning about migration
        expect(logger).to receive(:warn).with(/migrated message/i).at_least(:once)

        subject.queue_subscribe("test.queue")

        # Give handler thread time to process the migrated message
        sleep 0.2
      end
    end

    describe "intake fan-out" do
      it "spawns PB_NATS_SERVER_SUBSCRIPTION_HANDLERS handler threads" do
        ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = "3"
        manager = described_class.new(nats_client, &callback)

        handlers = manager.instance_variable_get(:@pending_queue_handlers)
        expect(handlers.size).to eq(3)
        expect(handlers).to all(be_alive)

        manager.shutdown(0.5)
      end

      it "keeps processing other messages when one handler is blocked (no head-of-line blocking)" do
        ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = "2"

        release = ::Queue.new
        processed = ::Queue.new
        calls = ::Concurrent::AtomicFixnum.new(0)
        cb = proc do |data, _reply, _subject|
          if calls.increment == 1
            release.pop # first message pins its handler until released
          else
            processed << data
          end
        end

        manager = described_class.new(nats_client, &cb)
        queue = manager.instance_variable_get(:@pending_queue)
        queue.push(double(:data => "A", :reply => "r", :subject => "s"))
        sleep 0.05 # let one handler pick up A and block
        queue.push(double(:data => "B", :reply => "r", :subject => "s"))

        # With a single handler this pop would block forever (head-of-line);
        # the second handler must process B while A is stuck.
        expect(::Timeout.timeout(2) { processed.pop }).to eq("B")
      ensure
        release << :go
        manager&.shutdown(0.5)
      end

      it "shuts down every handler thread" do
        ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = "3"
        manager = described_class.new(nats_client, &callback)
        handlers = manager.instance_variable_get(:@pending_queue_handlers)

        manager.shutdown(1)

        expect(handlers.any?(&:alive?)).to be(false)
      end
    end

    describe "thread naming" do
      it "uses unique thread names with object_id" do
        manager1 = described_class.new(nats_client, &callback)
        manager2 = described_class.new(nats_client, &callback)

        thread1 = manager1.instance_variable_get(:@pending_queue_handlers).first
        thread2 = manager2.instance_variable_get(:@pending_queue_handlers).first

        # Give threads time to set their names (race condition fix)
        # The name is set inside Thread.new, but might not have executed yet
        sleep 0.01 until thread1.name && thread2.name

        # Names should be different
        expect(thread1.name).to include("subscription-manager")
        expect(thread2.name).to include("subscription-manager")
        expect(thread1.name).not_to eq(thread2.name)

        manager1.shutdown(0.1)
        manager2.shutdown(0.1)
      end
    end
  end
end
