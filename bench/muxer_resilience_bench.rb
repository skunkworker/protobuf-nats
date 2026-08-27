# Benchmarks for the response-muxer hot-path and self-healing changes.
#
# This file measures BOTH the old (baseline) and new (patched) behavior in one
# process so the speedup/robustness delta is reproducible on CRuby and JRuby
# without a NATS server:
#
#   A. Dispatch hot-path cost   -- per-message pending_size accounting that was
#                                  removed (#1). benchmark-ips, lower is better.
#   B. nil-@resp_sub resilience -- busy-spin vs park during a restart window (#3).
#   C. Self-healing counter     -- lost updates with a plain int vs AtomicFixnum
#                                  under concurrent crashes (#4).
#
# Usage:
#   bundle exec ruby -Ilib bench/muxer_resilience_bench.rb

require "bundler/setup"
require "benchmark/ips"
require "concurrent"
require "nats/client" # real NATS::Subscription / NATS::Msg

def mono
  ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
end

puts "=" * 72
puts "protobuf-nats response-muxer resilience bench"
puts "engine=#{RUBY_ENGINE} #{RUBY_VERSION}  processor_count=#{::Concurrent.processor_count}"
puts "=" * 72

# --------------------------------------------------------------------------
# A. Dispatch hot-path: per-message pending_size accounting (removed in #1).
#
# Old dispatch did `sub.synchronize { sub.pending_size -= msg.data.size }` for
# EVERY response message; the new code does nothing here. We compare the old
# accounting step against the cheapest real per-message op (a Concurrent::Map
# lookup, which the dispatcher still does) so the delta is the lock overhead we
# removed from the hot path.
# --------------------------------------------------------------------------
puts "\nA. Dispatch hot-path per-message overhead (higher ips = better)\n\n"

sub = ::NATS::Subscription.new
sub.pending_size = 0
resp_map = ::Concurrent::Map.new
resp_map["tok"] = { :queue => ::Queue.new }
size = 64

Benchmark.ips do |x|
  x.config(:time => 3, :warmup => 1)

  x.report("old: synchronize { pending_size -= n } + map lookup") do
    sub.synchronize { sub.pending_size -= size }
    resp_map["tok"]
  end

  x.report("new: map lookup only (accounting removed)") do
    resp_map["tok"]
  end

  x.compare!
end

# --------------------------------------------------------------------------
# B. nil-@resp_sub resilience (#3). During a restart @resp_sub can briefly be
# nil. The old loop dereferenced it unconditionally (NoMethodError every
# iteration -> busy-spin + a logged error/callback per spin); the new loop
# parks. We run each for a fixed window and count iterations and "errors that
# would be logged/dispatched to callbacks".
# --------------------------------------------------------------------------
puts "\nB. Behavior while @resp_sub is nil for #{(WINDOW = 0.5)}s (lower spin = better)\n\n"

def run_old_loop(window)
  resp_sub = nil # the restart window
  iters = 0
  errors = 0
  deadline = mono + window
  while mono < deadline
    begin
      resp_sub.pending_queue.pop # NoMethodError on nil
    rescue => _e
      errors += 1 # old code logs + notify_error_callbacks here
    end
    iters += 1
  end
  [iters, errors]
end

def run_new_loop(window)
  resp_sub = nil
  iters = 0
  errors = 0
  deadline = mono + window
  while mono < deadline
    s = resp_sub
    if s.nil?
      sleep 0.01 # park instead of spinning
      iters += 1
      next
    end
    begin
      s.pending_queue.pop
    rescue => _e
      errors += 1
    end
    iters += 1
  end
  [iters, errors]
end

old_iters, old_errs = run_old_loop(WINDOW)
new_iters, new_errs = run_new_loop(WINDOW)

printf("  old loop: %12d iterations, %12d errors logged/dispatched\n", old_iters, old_errs)
printf("  new loop: %12d iterations, %12d errors logged/dispatched\n", new_iters, new_errs)
printf("  => new loop does %.5f%% of the old loop's wasted work\n",
       old_iters.zero? ? 0.0 : (new_iters.to_f / old_iters * 100))

# --------------------------------------------------------------------------
# C. Self-healing crash counter (#4). The old counter was a plain Integer
# mutated by multiple dispatcher threads (`@crash_count = (@crash_count||0)+1`),
# which loses updates under true parallelism, corrupting the exponential
# backoff. The new counter is a Concurrent::AtomicFixnum. We have N threads each
# "crash" K times and check the final count.
# --------------------------------------------------------------------------
puts "\nC. Crash-counter accuracy under concurrent crashes (expected == actual is correct)\n\n"

def hammer(counter, threads, per_thread)
  ts = threads.times.map do
    ::Thread.new do
      per_thread.times { counter.call }
    end
  end
  ts.each(&:join)
end

threads = [::Concurrent.processor_count, 4].max
per_thread = 50_000
expected = threads * per_thread

# Old: plain integer read-modify-write (racy).
plain = 0
hammer(->{ plain = plain + 1 }, threads, per_thread)

# New: atomic increment.
atomic = ::Concurrent::AtomicFixnum.new(0)
hammer(->{ atomic.increment }, threads, per_thread)

printf("  threads=%d  per_thread=%d  expected=%d\n", threads, per_thread, expected)
printf("  old plain Integer: %10d  (lost %d updates)\n", plain, expected - plain)
printf("  new AtomicFixnum:  %10d  (lost %d updates)\n", atomic.value, expected - atomic.value)

puts "\ndone."
