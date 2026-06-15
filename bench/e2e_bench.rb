# End-to-end throughput benchmark against a real nats-server.
#
# Starts a real protobuf-nats Server (warehouse service) in a background thread
# and drives it with N client threads for a fixed duration, reporting req/sec.
# Exercises the full patched stack: client ResponseMuxer + dispatchers, server
# SuperSubscriptionManager + ThreadPool.
#
# Requires a nats-server already running on 127.0.0.1:4222.
#
# Usage:
#   E2E_DURATION=10 E2E_THREADS=16 bundle exec ruby -Ilib bench/e2e_bench.rb

ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"
# Skip the server's slow-start ramp so the benchmark starts promptly.
ENV["PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT"] ||= "1"
ENV["PB_NATS_SERVER_SLOW_START_DELAY"] ||= "0"

require "./examples/warehouse/app"
require "concurrent"

::Protobuf::Logging.logger = ::Logger.new(nil)

DURATION       = Float(ENV.fetch("E2E_DURATION", "10"))
WARMUP         = Float(ENV.fetch("E2E_WARMUP", "3"))
THREADS        = Integer(ENV.fetch("E2E_THREADS", "16"))
SERVER_THREADS = Integer(ENV.fetch("E2E_SERVER_THREADS", "50"))

def mono
  ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
end

puts "=" * 72
puts "protobuf-nats e2e bench  engine=#{RUBY_ENGINE} #{RUBY_VERSION}"
puts "client_threads=#{THREADS} server_threads=#{SERVER_THREADS} duration=#{DURATION}s warmup=#{WARMUP}s"
puts "dispatchers=#{ENV['PB_NATS_RESPONSE_MUXER_DISPATCHERS'] || '(auto)'}"
puts "=" * 72

server = ::Protobuf::Nats::Server.new(:threads => SERVER_THREADS)
server_thread = ::Thread.new { server.run }

# Give the server time to connect + subscribe.
sleep 2

count   = ::Concurrent::AtomicFixnum.new(0)
errors  = ::Concurrent::AtomicFixnum.new(0)
measure = ::Concurrent::AtomicBoolean.new(false)
stop    = ::Concurrent::AtomicBoolean.new(false)

workers = THREADS.times.map do
  ::Thread.new do
    until stop.true?
      begin
        req = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :sleep_time_ms => 0)
        ::Warehouse::ShipmentService.client.create(req)
        count.increment if measure.true?
      rescue => _e
        errors.increment if measure.true?
      end
    end
  end
end

sleep WARMUP
t0 = mono
measure.make_true
sleep DURATION
stop.make_true
elapsed = mono - t0
workers.each(&:join)

rps = count.value / elapsed
puts
printf("req/sec = %.0f   (completed=%d errors=%d in %.2fs)\n", rps, count.value, errors.value, elapsed)

server.stop
server_thread.join(5)
puts "done."
