# Concurrency microbenchmarks for the protobuf-nats hot paths.
#
# Drives the REAL ResponseMuxer / Client subscription cache / ThreadPool so the
# same file measures both the baseline and the patched implementations, on both
# CRuby and JRuby. No NATS server required (NATS is faked for the muxer bench).
#
# Usage:
#   bundle exec ruby -Ilib bench/concurrency_bench.rb
#   BENCH_THREADS=1,4,8,16 BENCH_DURATION=4 BENCH_WARMUP=2 ruby -Ilib bench/concurrency_bench.rb
#
# Each cell runs for BENCH_DURATION seconds (after BENCH_WARMUP seconds of
# untimed warmup) and reports aggregate ops/sec across all threads plus an
# error count.

require "bundler/setup"
require "protobuf/nats"
require "nats/client" # for NATS::Msg / NATS::Subscription

::Protobuf::Logging.logger = ::Logger.new(nil)

DURATION = Float(ENV.fetch("BENCH_DURATION", "4"))
WARMUP   = Float(ENV.fetch("BENCH_WARMUP", "2"))
THREADS  = ENV.fetch("BENCH_THREADS", "1,4,8,16").split(",").map(&:to_i)
PAYLOAD  = ("x" * 64).freeze

def mono
  ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
end

# Run `block` (which returns [ops, errors]) on `threads` workers. Counters are
# only accumulated during the measured window (after an untimed warmup), using
# atomics so the stop/measure flags are visible across threads on JRuby too.
# Returns { ops_per_sec:, errors: }.
def run_cell(threads, seconds, warmup)
  go      = ::Concurrent::AtomicBoolean.new(false)
  measure = ::Concurrent::AtomicBoolean.new(false)
  stop    = ::Concurrent::AtomicBoolean.new(false)
  totals  = ::Array.new(threads)

  workers = threads.times.map do |i|
    ::Thread.new do
      ops = 0
      errs = 0
      sleep 0.0005 until go.true?
      until stop.true?
        o, e = yield
        if measure.true?
          ops += o
          errs += e
        end
      end
      totals[i] = [ops, errs]
    end
  end

  go.make_true
  sleep warmup
  t0 = mono
  measure.make_true
  sleep seconds
  stop.make_true
  elapsed = mono - t0
  workers.each(&:join)

  ops = totals.sum { |t| t ? t[0] : 0 }
  errs = totals.sum { |t| t ? t[1] : 0 }
  { ops_per_sec: ops / elapsed, errors: errs }
end

def print_table(title, rows)
  puts
  puts title
  puts "  threads |        ops/sec |  errors"
  puts "  --------+----------------+--------"
  rows.each do |t, r|
    printf("  %7d | %14.0f | %7d\n", t, r[:ops_per_sec], r[:errors])
  end
end

# --------------------------------------------------------------------------
# Fake NATS connection for the muxer round-trip benchmark.
# new_inbox + subscribe("<prefix>.*") feed the muxer; publish echoes a reply
# onto the muxer's response subscription queue, simulating the server response.
# --------------------------------------------------------------------------
class BenchNats
  def initialize
    @inbox = 0
    @resp_queue = nil
  end

  def new_inbox
    @inbox += 1
    "_INBOX.bench.#{@inbox}"
  end

  def subscribe(_subject, *_args)
    sub = ::NATS::Subscription.new
    sub.pending_queue = ::SizedQueue.new(8192)
    @resp_queue = sub.pending_queue # muxer's response subscription
    sub
  end

  def publish(_subject, data, reply_to = nil)
    return unless reply_to && @resp_queue
    @resp_queue.push(::NATS::Msg.new(:subject => reply_to, :data => data))
  end

  def flush(*); end
end

# --------------------------------------------------------------------------
# A. ResponseMuxer round-trip (exercises #1 map lock, #2 dispatcher, #4, #6)
# --------------------------------------------------------------------------
def bench_muxer
  rows = THREADS.map do |t|
    ::Protobuf::Nats.client_nats_connection = BenchNats.new
    muxer = ::Protobuf::Nats::ResponseMuxer.new
    muxer.start

    result = run_cell(t, DURATION, WARMUP) do
      ops = 0
      errs = 0
      begin
        req = muxer.new_request
        req.publish("rpc.bench", PAYLOAD)
        msg = req.next_message(5)
        ops += 1 if msg
      rescue => _e
        errs += 1
      ensure
        req.cleanup if req
      end
      [ops, errs]
    end

    muxer.stop
    [t, result]
  end
  print_table("A. ResponseMuxer round-trip (new_request -> publish -> next_message -> cleanup)", rows)
end

# --------------------------------------------------------------------------
# B. Subscription-key cache (exercises #3: nested-Hash ||= vs Concurrent::Map).
# Drives the real Client#cached_subscription_key. Each measured iteration clears
# the shared class cache and races all threads to refill it (the cold-start
# write race that triggers ConcurrentModificationError on JRuby + plain Hash).
# --------------------------------------------------------------------------
def bench_subscription_cache
  # Build named dummy service classes + methods used as cache keys.
  svc_classes = 20.times.map do |i|
    name = "BenchSvc#{i}"
    ::Object.const_set(name, Class.new) unless ::Object.const_defined?(name)
    ::Object.const_get(name)
  end
  methods = [:create, :read, :update, :delete, :list]
  combos = svc_classes.product(methods)

  cache = ::Protobuf::Nats::Client.subscription_key_cache

  rows = THREADS.map do |t|
    iteration_lock = ::Mutex.new
    result = run_cell(t, DURATION, WARMUP) do
      ops = 0
      errs = 0
      # Clear occasionally to keep the write path hot (cold-fill race).
      iteration_lock.synchronize { cache.clear } if rand(combos.size) == 0
      combos.each do |klass, meth|
        begin
          client = ::Protobuf::Nats::Client.allocate
          client.instance_variable_set(:@options, { :service => klass, :method => meth })
          client.cached_subscription_key
          ops += 1
        rescue => _e
          errs += 1
        end
      end
      [ops, errs]
    end
    [t, result]
  end
  print_table("B. Subscription-key cache fill+read race (Client#cached_subscription_key)", rows)
end

# --------------------------------------------------------------------------
# C. ThreadPool throughput (exercises #5: mutex counter vs AtomicFixnum)
# --------------------------------------------------------------------------
def bench_thread_pool
  workers = (ENV["BENCH_POOL_WORKERS"] || "8").to_i
  noop = ->{}
  rows = THREADS.map do |t|
    pool = ::Protobuf::Nats::ThreadPool.new(workers, :max_queue => 100_000)
    result = run_cell(t, DURATION, WARMUP) do
      ops = 0
      errs = 0
      if pool.push(&noop)
        ops += 1
      else
        errs += 1 # pool full (backpressure); not a real error, tracked separately
      end
      [ops, errs]
    end
    pool.shutdown
    [t, result]
  end
  print_table("C. ThreadPool push throughput (errors column = pool-full/backpressure)", rows)
end

puts "=" * 72
puts "protobuf-nats concurrency bench"
puts "engine=#{RUBY_ENGINE} #{RUBY_VERSION}  duration=#{DURATION}s warmup=#{WARMUP}s threads=#{THREADS.inspect}"
puts "processor_count=#{::Concurrent.processor_count}"
puts "=" * 72

bench_muxer
bench_subscription_cache
bench_thread_pool

puts
puts "done."
