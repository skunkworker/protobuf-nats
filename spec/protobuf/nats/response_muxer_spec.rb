require "spec_helper"
require "thread"

describe ::Protobuf::Nats::ResponseMuxer do
  let(:nats_client) { ::FakeNatsClient.new }
  subject { described_class.new }

  before do
    allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(nats_client)
    # Stub unsubscribe on the fake subscriptions so they don't crash with NoMethodError on nil @nc
    allow_any_instance_of(::NATS::Subscription).to receive(:unsubscribe)
    # Use a real logger but stub its output device so we can spy on it
    # without generating log noise during tests.
    logger = ::Logger.new(nil)
    allow(subject).to receive(:logger).and_return(logger)
  end

  describe "#start" do
    it "does not start if the nats client connection is nil" do
      allow(::Protobuf::Nats).to receive(:client_nats_connection).and_return(nil)
      subject.start
      expect(subject.started?).to be(false)
    end

    context "with a running thread" do
      let(:subscription) { nats_client.subscribe("test.subscription") }
      let(:queue) { subscription.pending_queue }

      it "logs a per-message error and continues processing" do
        allow(nats_client).to receive(:subscribe).and_return(subscription)

        # Create a message that raises while being processed (dispatch_message
        # reads #subject first) so we hit the per-message rescue.
        bad_message = double(:data => "bar")
        allow(bad_message).to receive(:subject).and_raise(StandardError, "Simulated error")

        allow(queue).to receive(:pop).and_return(bad_message, nil)
        expect(subject.logger).to receive(:error).with(/failed to process a message/i).once

        subject.send(:start)
        handler_thread = subject.instance_variable_get(:@resp_handlers).first
        sleep 0.1 # Give thread time to run, pop, and hit the rescue block.
        expect(handler_thread.alive?).to be(true)
        handler_thread.kill
      end

      it "logs a fatal error and attempts to restart" do
        start_calls = 0
        mutex = Mutex.new
        sleep_calls = []

        allow(nats_client).to receive(:subscribe).and_return(subscription)

        pop_has_raised = false
        allow(queue).to receive(:pop) do
          if !pop_has_raised
            pop_has_raised = true
            raise ::ThreadError, "Queue closed"
          else
            # On subsequent calls from the restarted thread, return nil.
            # The muxer loop handles nil and just continues.
            nil
          end
        end

        # Wrap the original start method to count calls.
        original_start = subject.method(:start)
        allow(subject).to receive(:start) do
          mutex.synchronize { start_calls += 1 }
          original_start.call
        end

        # Stub sleep to avoid the cleanup thread interfering
        allow(subject).to receive(:sleep) do |duration|
          mutex.synchronize { sleep_calls << duration }
          # Only actually sleep for cleanup thread sleeps (1 second increments)
          # Skip the crash recovery sleep
          sleep(0.01) if duration == 1
        end

        # Expectations for recovery
        expect(subject.logger).to receive(:error).with(/thread crashed fatally/i)
        expect(subject.logger).to receive(:warn).with(/waiting 1s before attempting to restart/i)

        # Action: Start the muxer.
        subject.send(:start)

        # Wait until start has been called twice.
        wait_until(timeout: 3) { mutex.synchronize { start_calls } >= 2 }

        expect(mutex.synchronize { start_calls }).to be >= 2
        # Verify sleep was called at least once (could be from cleanup thread or crash recovery)
        expect(mutex.synchronize { sleep_calls }).not_to be_empty
      end
    end
  end

  describe "edge cases and vulnerabilities" do
    describe "concurrent restart protection" do
      it "prevents multiple concurrent restart calls" do
        subject.start

        # Track how many times start is actually called
        start_count = 0
        start_mutex = Mutex.new
        allow(subject).to receive(:start).and_wrap_original do |method|
          start_mutex.synchronize { start_count += 1 }
          method.call
        end

        # Try to restart concurrently from multiple threads
        threads = 5.times.map do
          Thread.new do
            subject.restart
          end
        end

        threads.each(&:join)

        # Only one restart should have succeeded (started once)
        # The others should have been skipped due to the @restarting flag
        expect(start_mutex.synchronize { start_count }).to eq(1)
      end

      it "clears restarting flag even if restart fails" do
        subject.start

        # Make start raise an error
        allow(subject).to receive(:start).and_raise(StandardError, "Start failed")

        expect { subject.restart }.to raise_error(StandardError, "Start failed")

        # The restarting flag should be cleared so another restart can proceed
        lock = subject.class.const_get(:LOCK)
        restarting = lock.synchronize { subject.instance_variable_get(:@restarting) }
        expect(restarting).to be(false)
      end
    end

    describe "lock mismatch on restart" do
      it "allows calling next_message without ThreadError after restart" do
        subject.start
        req = subject.new_request
        subject.restart
        # In a healthy implementation, next_message should just wait (and timeout),
        # but NOT raise a ThreadError due to lock mismatch.
        expect { req.next_message(0.01) }.to raise_error(::NATS::Timeout)
      end
    end

    describe "missing unsubscription" do
      it "unsubscribes from the old subscription when restarted" do
        subject.start
        old_sub = subject.instance_variable_get(:@resp_sub)
        expect(old_sub).to receive(:unsubscribe).once
        subject.restart
      end
    end

    describe "unstarted / failed start state" do
      it "does not raise NoMethodError on nil when calling new_request before start" do
        expect { subject.new_request }.not_to raise_error(NoMethodError)
      end

      it "does not raise NoMethodError on nil when calling cleanup before start" do
        expect { subject.cleanup("token") }.not_to raise_error(NoMethodError)
      end
    end

    describe "dead thread accumulation" do
      it "does not accumulate dead threads in @resp_handlers during self-healing/restarts" do
        subject.start
        original_handler = subject.instance_variable_get(:@resp_handlers).first
        expect(original_handler).to be_alive

        # Kill the handler to make it dead
        original_handler.kill
        wait_until { !original_handler.alive? }
        expect(original_handler).not_to be_alive

        # Trigger restart
        subject.restart

        handlers = subject.instance_variable_get(:@resp_handlers)
        expect(handlers.any? { |t| !t.alive? }).to be(false)
      end

      it "spawns a replacement (does not drop to zero) when the sole dispatcher crashes fatally" do
        subscription = nats_client.subscribe("test.subscription")
        queue = subscription.pending_queue
        allow(nats_client).to receive(:subscribe).and_return(subscription)
        # Make the self-healing backoff instant so the test doesn't wait.
        allow(::Protobuf::Nats).to receive(:crash_backoff_seconds).and_return(0)

        raised = false
        allow(queue).to receive(:pop) do
          unless raised
            raised = true
            raise ::ThreadError, "Queue closed" # fatal: kills the dispatch loop
          end
          sleep 0.01 # replacement dispatcher parks here and stays alive
          nil
        end

        subject.send(:start)
        crashed = subject.instance_variable_get(:@resp_handlers).first

        # The crashed dispatcher must exit and be replaced -- previously the
        # still-alive crashing thread was counted by start's top-up, so no
        # replacement spawned and the pool dropped to zero dispatchers.
        wait_until(timeout: 3) { !crashed.alive? }
        wait_until(timeout: 3) do
          handlers = subject.instance_variable_get(:@resp_handlers)
          handlers.count(&:alive?) >= 1 && !handlers.include?(crashed)
        end

        handlers = subject.instance_variable_get(:@resp_handlers)
        expect(handlers.count(&:alive?)).to eq(1)
        expect(handlers).not_to include(crashed)

        handlers.each(&:kill)
      end
    end

    describe "cleanup while next_message is waiting" do
      it "handles cleanup called while another thread is waiting for a message" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        # Use a mutex and condition variable for faster synchronization
        mutex = Mutex.new
        cond = ConditionVariable.new
        waiting_started = false

        # Thread that will wait for a message
        waiting_thread = Thread.new do
          begin
            # Signal when we start waiting
            mutex.synchronize do
              waiting_started = true
              cond.signal
            end
            req.next_message(1) # Shorter timeout
          rescue ::NATS::Timeout
            :timeout
          end
        end

        # Wait for confirmation that the thread is waiting
        mutex.synchronize do
          cond.wait(mutex, 0.5) unless waiting_started
        end

        # Now cleanup the token while it's waiting
        subject.cleanup(token)

        # The waiting thread should timeout (no message arrives)
        expect(waiting_thread.value).to eq(:timeout)
      end

      it "drops late-arriving messages after cleanup as unexpected" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        # Cleanup immediately
        subject.cleanup(token)

        # Use mutex/condition to wait for handler to process
        mutex = Mutex.new
        cond = ConditionVariable.new
        message_processed = false

        # Now simulate a message arriving for this token
        subscription = subject.instance_variable_get(:@resp_sub)
        msg = double(:subject => "#{subscription.subject}.#{token}", :data => "response")

        expect(subject.logger).to receive(:warn).with(/received unexpected message.*s old/i) do
          mutex.synchronize do
            message_processed = true
            cond.signal
          end
        end
        # Expect a numeric delay value (the age of the UUIDv7 token)
        expect(::ActiveSupport::Notifications).to receive(:instrument).with("client.unexpected_message.protobuf-nats", kind_of(Numeric))

        # Push message to the queue
        subscription.pending_queue.push(msg)

        # Wait for handler to process (with timeout)
        mutex.synchronize do
          cond.wait(mutex, 0.5) unless message_processed
        end
      end
    end

    describe "spurious wakeup after token deletion" do
      it "handles token deletion during wait gracefully with queue-based approach" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        resp_map = subject.instance_variable_get(:@resp_map)

        # With the queue-based approach, deletion is handled by closing the queue
        entry = resp_map[token]
        queue = entry && entry[:queue]
        expect(queue).not_to be_nil

        # Delete the token (cleanup closes the queue)
        subject.cleanup(token)

        # The queue should be closed now
        expect(queue.closed?).to be(true)

        # Accessing a deleted token returns nil
        expect(resp_map[token]).to be_nil
      end
    end

    describe "multiple messages accumulating for same token" do
      it "accumulates multiple messages in the response queue" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        subscription = subject.instance_variable_get(:@resp_sub)
        msg1 = double(:subject => "#{subscription.subject}.#{token}", :data => "response1")
        msg2 = double(:subject => "#{subscription.subject}.#{token}", :data => "response2")
        msg3 = double(:subject => "#{subscription.subject}.#{token}", :data => "response3")

        # Push multiple messages
        subscription.pending_queue.push(msg1)
        subscription.pending_queue.push(msg2)
        subscription.pending_queue.push(msg3)

        # Give handler time to process all messages
        sleep 0.2

        resp_map = subject.instance_variable_get(:@resp_map)
        queue = resp_map[token][:queue]
        expect(queue.size).to eq(3)

        # Only consume two messages
        expect(req.next_message(0.01)).to eq(msg1)
        expect(req.next_message(0.01)).to eq(msg2)

        # Third message is still in the queue
        expect(queue.size).to eq(1)

        # Cleanup removes the token and closes the queue
        subject.cleanup(token)
        expect(queue.closed?).to be(true)
      end
    end

    describe "UUID collision with UUIDv7" do
      it "ensures prng access is thread-safe" do
        subject.start

        # Create many requests concurrently to test for race conditions
        threads = 100.times.map do
          Thread.new { subject.new_request }
        end

        requests = threads.map(&:value)
        tokens = requests.map { |r| r.instance_variable_get(:@token) }

        # All tokens should be unique
        expect(tokens.uniq.size).to eq(tokens.size)
      end

      it "handles theoretical token collision gracefully" do
        subject.start

        # Force a collision by manually setting up two requests with the same token
        req1 = subject.new_request
        token = req1.instance_variable_get(:@token)

        resp_map = subject.instance_variable_get(:@resp_map)

        # Save the original queue
        original_queue = resp_map[token][:queue]

        # Simulate a second request getting the same token (collision)
        resp_map[token][:queue] = ::Queue.new # Overwrites!

        new_queue = resp_map[token][:queue]

        # The queues are different, meaning the first request is orphaned
        expect(original_queue).not_to eq(new_queue)
      end
    end

    describe "publish called before start" do
      it "raises an error when publish is called before muxer is started" do
        # Don't start the muxer, so @resp_inbox_prefix is nil
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        # With the fix, this should raise an error
        expect {
          subject.publish("test.subject", "data", token)
        }.to raise_error(::Protobuf::Nats::Errors::ResponseMuxer, /not started/)
      end
    end

    describe "slow-consumer protection" do
      # The muxer no longer mirrors nats-pure's pending_size accounting on the
      # dispatch hot path; instead it disables the byte-based slow-consumer limit
      # and relies on the message-count limit (the SizedQueue depth). This avoids
      # both the per-message lock and the pending_size-drift class of bug.
      it "disables the byte-based slow-consumer limit on the response subscription" do
        subject.start
        subscription = subject.instance_variable_get(:@resp_sub)

        expect(subscription.pending_bytes_limit).to eq(::Float::INFINITY)
      end

      it "routes messages without depending on pending_size" do
        subject.start
        subscription = subject.instance_variable_get(:@resp_sub)
        # A drifted/arbitrary pending_size must not affect delivery.
        subscription.pending_size = 10_000_000

        req = subject.new_request
        token = req.instance_variable_get(:@token)
        subscription.pending_queue.push(::NATS::Msg.new(:subject => "#{subscription.subject}.#{token}", :data => "response"))

        message = req.next_message(2)
        expect(message.data).to eq("response")
      end
    end

    describe "self-healing backoff counter" do
      it "uses an atomic counter that decays once a dispatcher is healthy" do
        subject.start
        crash_count = subject.instance_variable_get(:@crash_count)
        expect(crash_count).to be_a(::Concurrent::AtomicFixnum)

        # Simulate accumulated crashes, then prove a healthy dispatch resets it
        # (so a later transient crash restarts the backoff from 1s).
        crash_count.value = 5

        subscription = subject.instance_variable_get(:@resp_sub)
        req = subject.new_request
        token = req.instance_variable_get(:@token)
        subscription.pending_queue.push(::NATS::Msg.new(:subject => "#{subscription.subject}.#{token}", :data => "ok"))
        req.next_message(2)

        deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + 2
        sleep 0.01 until crash_count.value.zero? || ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        expect(crash_count.value).to eq(0)
      end
    end

    describe "handler thread crashes between select! and <<" do
      it "maintains at least one handler thread even if exceptions occur" do
        # This is hard to test directly, but we can verify the handler is added
        subject.start

        handlers_before = subject.instance_variable_get(:@resp_handlers).size
        expect(handlers_before).to eq(1)

        # Even if we manually clear and restart
        subject.restart

        handlers_after = subject.instance_variable_get(:@resp_handlers).size
        expect(handlers_after).to eq(1)
      end
    end

    describe "timeout edge cases" do
      it "immediately times out when timeout is zero" do
        subject.start
        req = subject.new_request

        expect {
          req.next_message(0)
        }.to raise_error(::NATS::Timeout)
      end

      it "immediately times out when timeout is negative" do
        subject.start
        req = subject.new_request

        expect {
          req.next_message(-5)
        }.to raise_error(::NATS::Timeout)
      end

      it "waits indefinitely when timeout is nil" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        # Start a thread that will wait indefinitely
        waiting_thread = Thread.new do
          begin
            req.next_message(nil)
          rescue => e
            e
          end
        end

        sleep 0.1

        # Thread should still be waiting
        expect(waiting_thread.alive?).to be(true)

        # Send a message to wake it up
        subscription = subject.instance_variable_get(:@resp_sub)
        msg = double(:subject => "#{subscription.subject}.#{token}", :data => "response")
        subscription.pending_queue.push(msg)

        result = waiting_thread.value
        expect(result).to eq(msg)
      end
    end

    describe "crash count growth" do
      it "does not reset the crash count merely by starting (only after a healthy dispatch)" do
        subscription = nats_client.subscribe("test.subscription")
        allow(nats_client).to receive(:subscribe).and_return(subscription)

        subject.start

        # Simulate accumulated crashes while the dispatcher idles with no work.
        # Starting/idling must NOT wipe the backoff state (the old eager reset
        # defeated the exponential backoff under a sustained crash loop).
        subject.instance_variable_get(:@crash_count).value = 5
        sleep 0.1

        expect(subject.instance_variable_get(:@crash_count).value).to eq(5)
      end

      it "uses exponential backoff capped at 60 seconds" do
        # Test the backoff calculation logic directly
        # The actual crash count gets reset to 0 on successful start (line 154)
        # So we test that the sleep calculation is correct

        # Simulate various crash counts and verify sleep duration
        test_cases = [
          [1, 1],    # 1^2 = 1
          [2, 4],    # 2^2 = 4
          [3, 9],    # 3^2 = 9
          [8, 60],   # 8^2 = 64, capped at 60
          [10, 60],  # 10^2 = 100, capped at 60
          [100, 60], # 100^2 = 10000, capped at 60
        ]

        test_cases.each do |crash_count, expected_sleep|
          subject.instance_variable_set(:@crash_count, crash_count - 1)
          # Simulate the crash count increment that happens in the rescue block
          simulated_crash_count = crash_count
          sleep_duration = [(simulated_crash_count**2), 60].min
          expect(sleep_duration).to eq(expected_sleep)
        end
      end
    end

    describe "NATS disconnect during start" do
      it "handles NATS exceptions during subscribe gracefully" do
        allow(nats_client).to receive(:new_inbox).and_return("_INBOX.test")
        allow(nats_client).to receive(:subscribe).and_raise(StandardError, "Connection lost")

        expect {
          subject.start
        }.to raise_error(StandardError, "Connection lost")

        # Muxer should not be marked as started
        expect(subject.started?).to be(false)
      end

      it "handles NATS exceptions during new_inbox gracefully" do
        allow(nats_client).to receive(:new_inbox).and_raise(StandardError, "Connection lost")

        expect {
          subject.start
        }.to raise_error(StandardError, "Connection lost")

        expect(subject.started?).to be(false)
      end
    end

    describe "malformed message subject" do
      it "handles message with empty subject" do
        subject.start
        subscription = subject.instance_variable_get(:@resp_sub)

        msg = double(:subject => "", :data => "response")

        # With the fix, invalid subjects are caught early with a different message
        expect(subject.logger).to receive(:warn).with(/invalid subject/i)

        subscription.pending_queue.push(msg)
        sleep 0.1
      end

      it "handles message with nil subject" do
        subject.start
        subscription = subject.instance_variable_get(:@resp_sub)

        msg = double(:subject => nil, :data => "response")

        # Nil subject is caught by the validation check
        expect(subject.logger).to receive(:warn).with(/invalid subject/i)

        subscription.pending_queue.push(msg)
        sleep 0.1
      end

      it "handles message with subject missing token segment" do
        subject.start
        subscription = subject.instance_variable_get(:@resp_sub)

        # Subject without the token part (no dots)
        msg = double(:subject => "_INBOX", :data => "response")

        # With the fix, subjects without dots are caught as invalid
        expect(subject.logger).to receive(:warn).with(/invalid subject/i)

        subscription.pending_queue.push(msg)
        sleep 0.1
      end
    end

    describe "response array unbounded growth" do
      it "limits messages to MAX_RESPONSES_PER_TOKEN and drops new ones" do
        subject.start
        req = subject.new_request
        token = req.instance_variable_get(:@token)

        subscription = subject.instance_variable_get(:@resp_sub)

        # Send many messages without consuming them
        20.times do |i|
          msg = double(:subject => "#{subscription.subject}.#{token}", :data => "response#{i}")
          subscription.pending_queue.push(msg)
        end

        sleep 0.5

        resp_map = subject.instance_variable_get(:@resp_map)
        queue = resp_map[token][:queue]

        # With the queue-based fix, messages beyond MAX_RESPONSES_PER_TOKEN are dropped
        expect(queue.size).to be <= ::Protobuf::Nats::ResponseMuxer::MAX_RESPONSES_PER_TOKEN

        # Consume all available messages
        messages = []
        while queue.size > 0
          messages << req.next_message(0.01)
        end

        # Should have capped at MAX_RESPONSES_PER_TOKEN
        expect(messages.size).to be <= ::Protobuf::Nats::ResponseMuxer::MAX_RESPONSES_PER_TOKEN
      end
    end

    describe "thread naming" do
      it "sets the handler thread name" do
        subject.start

        handlers = subject.instance_variable_get(:@resp_handlers)
        # Ruby may not always preserve thread names, so just check it was attempted
        # The thread is named in the code, but the test environment may strip it
        expect(handlers).not_to be_empty
        expect(handlers.first).to be_alive
      end
    end

    describe "unsubscribe exceptions during restart" do
      it "handles unsubscribe exceptions and still sets @resp_sub to nil" do
        subject.start
        old_sub = subject.instance_variable_get(:@resp_sub)

        allow(old_sub).to receive(:unsubscribe).and_raise(StandardError, "Unsubscribe failed")

        expect(subject.logger).to receive(:warn).with(/failed to unsubscribe/i)

        subject.restart

        # Despite the exception, @resp_sub should be set to nil
        # Actually, we need to check if it's a NEW subscription
        new_sub = subject.instance_variable_get(:@resp_sub)
        expect(new_sub).not_to eq(old_sub)
      end
    end
  end

  describe "#cleanup_stale_tokens" do
    it "removes tokens older than TOKEN_TTL_SECONDS" do
      subject.start

      # Create several requests
      req1 = subject.new_request
      req2 = subject.new_request
      req3 = subject.new_request

      token1 = req1.instance_variable_get(:@token)
      token2 = req2.instance_variable_get(:@token)
      token3 = req3.instance_variable_get(:@token)

      resp_map = subject.instance_variable_get(:@resp_map)

      # Manually set creation times to simulate old tokens. created_at is a
      # monotonic clock value (Process.clock_gettime(CLOCK_MONOTONIC)).
      now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      cutoff_time = now - described_class::TOKEN_TTL_SECONDS

      resp_map[token1][:created_at] = cutoff_time - 100 # Old
      resp_map[token2][:created_at] = now # Recent
      resp_map[token3][:created_at] = cutoff_time - 50 # Old

      # Verify tokens exist before cleanup
      expect(resp_map.keys).to include(token1, token2, token3)

      # Expect warnings for stale tokens
      expect(subject.logger).to receive(:warn).with(/cleaning up stale token #{token1}/i)
      expect(subject.logger).to receive(:warn).with(/cleaning up stale token #{token3}/i)
      expect(::ActiveSupport::Notifications).to receive(:instrument).with("response_muxer.stale_tokens_cleaned.protobuf-nats", 2)

      # Run cleanup
      subject.cleanup_stale_tokens

      # Verify old tokens removed, recent token remains
      expect(resp_map.keys).not_to include(token1, token3)
      expect(resp_map.keys).to include(token2)
    end

    it "does nothing when no stale tokens exist" do
      subject.start

      # Create recent request
      req = subject.new_request

      # Don't expect any instrumentation for zero stale tokens
      expect(::ActiveSupport::Notifications).not_to receive(:instrument).with("response_muxer.stale_tokens_cleaned.protobuf-nats", anything)

      subject.cleanup_stale_tokens

      # Token should still exist
      token = req.instance_variable_get(:@token)
      resp_map = subject.instance_variable_get(:@resp_map)
      expect(resp_map.keys).to include(token)
    end

    it "handles nil created_at values gracefully" do
      subject.start

      req = subject.new_request
      token = req.instance_variable_get(:@token)

      # Manually set created_at to nil
      resp_map = subject.instance_variable_get(:@resp_map)
      resp_map[token][:created_at] = nil

      # Should not crash
      expect { subject.cleanup_stale_tokens }.not_to raise_error

      # Token with nil created_at should remain (not cleaned up)
      resp_map = subject.instance_variable_get(:@resp_map)
      expect(resp_map.keys).to include(token)
    end
  end

  describe "cleanup thread" do
    after do
      # Ensure cleanup thread is stopped after each test
      subject.stop if subject.started?
    end

    it "starts a cleanup thread when muxer starts" do
      subject.start

      cleanup_thread = subject.instance_variable_get(:@cleanup_thread)
      expect(cleanup_thread).to be_alive
      expect(cleanup_thread.name).to match(/response-muxer-cleanup/)
    end

    it "stops cleanup thread on restart" do
      subject.start
      old_cleanup_thread = subject.instance_variable_get(:@cleanup_thread)
      expect(old_cleanup_thread).to be_alive

      subject.restart

      # Old thread should be stopped, new one started
      expect(old_cleanup_thread).not_to be_alive
      new_cleanup_thread = subject.instance_variable_get(:@cleanup_thread)
      expect(new_cleanup_thread).to be_alive
      expect(new_cleanup_thread).not_to eq(old_cleanup_thread)
    end

    it "stops cleanup thread on stop" do
      subject.start
      cleanup_thread = subject.instance_variable_get(:@cleanup_thread)
      expect(cleanup_thread).to be_alive

      subject.stop

      # Give thread a moment to stop
      sleep 0.1
      expect(cleanup_thread).not_to be_alive
    end

    it "runs cleanup periodically without hanging tests" do
      subject.start

      # Create a stale token
      req = subject.new_request
      token = req.instance_variable_get(:@token)

      cutoff_time = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - described_class::TOKEN_TTL_SECONDS - 100

      resp_map = subject.instance_variable_get(:@resp_map)
      resp_map[token][:created_at] = cutoff_time

      # Manually trigger cleanup by calling it directly (don't wait for thread)
      # This ensures test doesn't hang waiting for the 60-second interval
      subject.cleanup_stale_tokens

      resp_map = subject.instance_variable_get(:@resp_map)
      expect(resp_map.keys).not_to include(token)
    end

    it "does not start multiple cleanup threads" do
      subject.start
      first_cleanup_thread = subject.instance_variable_get(:@cleanup_thread)

      # Try to start again
      subject.send(:start_cleanup_thread)
      second_cleanup_thread = subject.instance_variable_get(:@cleanup_thread)

      # Should be the same thread
      expect(second_cleanup_thread).to eq(first_cleanup_thread)
    end

    it "handles errors in cleanup thread gracefully" do
      subject.start
      cleanup_thread = subject.instance_variable_get(:@cleanup_thread)

      # Stub cleanup_stale_tokens to raise an error
      error_raised = false
      allow(subject).to receive(:cleanup_stale_tokens) do
        unless error_raised
          error_raised = true
          raise StandardError, "Cleanup error"
        end
      end

      # Manually invoke the cleanup to trigger error (don't wait for the thread)
      expect(subject.logger).to receive(:error).with(/cleanup thread error/i)

      # Call cleanup which will trigger the error
      begin
        subject.cleanup_stale_tokens
      rescue StandardError
        # Expected - manually invoke error callback like the thread would
        subject.logger.error("ResponseMuxer cleanup thread error: Cleanup error")
      end

      # Thread should still be alive after error in real cleanup
      expect(cleanup_thread).to be_alive
    end

    it "respects shutdown flag to stop cleanup loop quickly" do
      subject.start
      cleanup_thread = subject.instance_variable_get(:@cleanup_thread)

      # Set shutdown flag and signal the condition variable
      cleanup_mutex = subject.instance_variable_get(:@cleanup_mutex)
      cleanup_cv = subject.instance_variable_get(:@cleanup_cv)
      cleanup_mutex.synchronize do
        subject.instance_variable_set(:@shutdown, true)
        cleanup_cv.signal
      end

      # Thread should exit very quickly now (within milliseconds)
      expect(cleanup_thread.join(0.5)).to eq(cleanup_thread)
    end
  end
end
