# Benchmarks for the server intake fan-out (#1) and handler observability (#2).
#
# Models old (1 intake handler) vs new (N intake handlers) in one process, plus
# a demonstration of the #2 in-flight observability. No NATS server required.
#
#   A. Intake throughput     -- acks/sec with 1 vs N drain threads when each ACK
#                               publish has some latency (the real bottleneck).
#   B. Head-of-line blocking -- how long other subjects stall behind one slow
#                               publish with 1 vs N handlers.
#   C. Observability demo    -- with hung handlers, the new server notifications
#                               surface the saturation/overdue work that was
#                               previously invisible (only message_dropped).
#
# Usage:
#   bundle exec ruby -Ilib bench/server_intake_bench.rb

require "bundler/setup"
require "concurrent"
require "nats/client"             # real NATS::Subscription / NATS::Msg
require "protobuf/nats"

::Protobuf::Logging.logger = ::Logger.new(nil)

def mono
  ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
end

HANDLERS        = Integer(ENV.fetch("BENCH_HANDLERS", [::Concurrent.processor_count, 4].max.to_s))
MSGS            = Integer(ENV.fetch("BENCH_MSGS", "20000"))
PUBLISH_LAT_US  = Integer(ENV.fetch("BENCH_PUBLISH_LATENCY_US", "50")) # per-ACK publish latency

puts "=" * 72
puts "protobuf-nats server intake bench"
puts "engine=#{RUBY_ENGINE} #{RUBY_VERSION}  processor_count=#{::Concurrent.processor_count}"
puts "handlers(new)=#{HANDLERS}  msgs=#{MSGS}  publish_latency=#{PUBLISH_LAT_US}us"
puts "=" * 72

# --------------------------------------------------------------------------
# Shared intake model: a SizedQueue fed with `total` messages, drained by
# `handlers` threads. Each message does light work + an ACK "publish" that
# costs `publish_latency` seconds (the part that serializes on one thread today).
# --------------------------------------------------------------------------
def drain(handlers, total, publish_latency)
  queue = ::SizedQueue.new(total + handlers)
  total.times { queue.push(:msg) }
  handlers.times { queue.push(:stop) }
  processed = ::Concurrent::AtomicFixnum.new(0)

  t0 = mono
  threads = handlers.times.map do
    ::Thread.new do
      loop do
        m = queue.pop
        break if m == :stop
        sleep(publish_latency) if publish_latency.positive?
        processed.increment
      end
    end
  end
  threads.each(&:join)
  elapsed = mono - t0
  { per_sec: processed.value / elapsed, elapsed: elapsed }
end

puts "\nA. Intake throughput (acks/sec; higher is better)\n\n"
lat = PUBLISH_LAT_US / 1_000_000.0
old = drain(1, MSGS, lat)
new = drain(HANDLERS, MSGS, lat)
printf("  old (1 handler):   %12.0f acks/s  (%.2fs)\n", old[:per_sec], old[:elapsed])
printf("  new (%d handlers):  %12.0f acks/s  (%.2fs)\n", HANDLERS, new[:per_sec], new[:elapsed])
printf("  => %.2fx faster intake\n", new[:per_sec] / old[:per_sec])

# --------------------------------------------------------------------------
# B. Head-of-line blocking: one slow publish is enqueued first, followed by
# `fast_count` quick messages. Measure how long until all the quick messages
# finish. With one handler they wait behind the slow publish; with N they don't.
# --------------------------------------------------------------------------
def head_of_line(handlers, slow_latency, fast_count)
  queue = ::SizedQueue.new(fast_count + 1 + handlers)
  queue.push(:slow)
  fast_count.times { queue.push(:fast) }
  handlers.times { queue.push(:stop) }

  fast_done = ::Concurrent::AtomicFixnum.new(0)
  last_fast_at = ::Concurrent::AtomicReference.new(nil)

  start = mono
  threads = handlers.times.map do
    ::Thread.new do
      loop do
        m = queue.pop
        break if m == :stop
        if m == :slow
          sleep slow_latency
        else
          last_fast_at.set(mono) if fast_done.increment == fast_count
        end
      end
    end
  end
  threads.each(&:join)
  (last_fast_at.get || mono) - start
end

puts "\nB. Head-of-line blocking behind one slow (0.5s) publish (lower = better)\n\n"
slow = 0.5
old_b = head_of_line(1, slow, 50)
new_b = head_of_line(HANDLERS, slow, 50)
printf("  old (1 handler):   50 quick messages finished after %6.1f ms (stuck behind the slow publish)\n", old_b * 1000)
printf("  new (%d handlers):  50 quick messages finished after %6.1f ms (unaffected)\n", HANDLERS, new_b * 1000)

# --------------------------------------------------------------------------
# C. #2 observability demo: hung handlers occupy the pool. Today operators only
# see `message_dropped`; now the in-flight gauges + overdue event explain why.
# --------------------------------------------------------------------------
puts "\nC. Handler-exhaustion observability (what an operator now sees)\n\n"

ENV["PB_NATS_SERVER_SUBSCRIPTION_HANDLERS"] = "1"
ENV["PB_NATS_SERVER_HANDLER_OVERDUE_MS"] = "100"

class DemoNats
  def connect(*); end
  def new_inbox; "_INBOX.demo"; end
  def subscribe(_s, *_a)
    sub = ::NATS::Subscription.new
    sub.pending_queue = ::SizedQueue.new(1024)
    sub
  end
  def publish(*); end
  def flush(*); end
  %i[on_disconnect on_reconnect on_close on_error].each { |m| define_method(m) { |*| } }
  def close; end
end

server = ::Protobuf::Nats::Server.new(:threads => 4, :client => DemoNats.new, :server => "bench")
release = ::Queue.new
server.define_singleton_method(:handle_request) { |*_| release.pop; "" }

gauges = {}
%w[inflight_count inflight_oldest_age_ms overdue_handler_count handler_overdue pending_intake_queue_size].each do |name|
  ::ActiveSupport::Notifications.subscribe("server.#{name}.protobuf-nats") { |_, _, _, _, v| gauges[name] = v }
end

4.times { |i| server.enqueue_request("req#{i}", "inbox#{i}") } # all 4 pool slots now hung
sleep 0.15                                                       # exceed the 100ms overdue window
server.enqueue_request("req5", "inbox5")                         # pool full -> NACK + saturated
server.instrument_inflight_handlers

printf("  inflight_count          = %s   (handlers stuck on the downstream)\n", gauges["inflight_count"])
printf("  inflight_oldest_age_ms  = %.0f\n", gauges["inflight_oldest_age_ms"] || 0)
printf("  overdue_handler_count   = %s   (client already gave up on these)\n", gauges["overdue_handler_count"])
printf("  handler_overdue fired   = %s\n", gauges.key?("handler_overdue"))
puts   "  (previously: only server.message_dropped, with no hint that handlers were stuck)"

release << :go while !release.num_waiting.zero?
4.times { release << :go }

puts "\ndone."
