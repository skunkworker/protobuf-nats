# Soak / chaos test for protobuf-nats.
#
# Runs a real protobuf-nats server + client in one process against a real
# nats-server, drives sustained concurrent RPCs (including deliberately long
# handlers), and induces chaos by bouncing the nats-server mid-run. It then
# asserts the system recovers (the vast majority of requests still succeed) and
# prints the resilience signals it observed.
#
# This is an opt-in tool (like bench/real_client.rb), not part of the suite. It
# self-skips if `nats-server` isn't on PATH.
#
# Usage:
#   bundle exec ruby -Ilib bench/soak.rb
#   SOAK_DURATION=20 SOAK_THREADS=16 SOAK_BOUNCES=3 bundle exec ruby -Ilib bench/soak.rb

require "bundler/setup"
require "fileutils"
require "socket"
require "securerandom"
require "yaml"
require "concurrent"

unless system("which nats-server > /dev/null 2>&1")
  puts "[soak] nats-server not found on PATH -- skipping. (brew install nats-server)"
  exit 0
end

DURATION = Integer(ENV.fetch("SOAK_DURATION", "15"))
THREADS  = Integer(ENV.fetch("SOAK_THREADS", "12"))
BOUNCES  = Integer(ENV.fetch("SOAK_BOUNCES", "2"))
PORT     = Integer(ENV.fetch("SOAK_NATS_PORT", "4299"))

ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"
# Point the client/server at our throwaway nats-server.
config_path = ::File.expand_path("../tmp/soak_protobuf_nats.yml", __dir__)
::FileUtils.mkdir_p(::File.dirname(config_path))
::File.write(config_path, { "development" => { "servers" => ["nats://127.0.0.1:#{PORT}"], "max_reconnect_attempts" => 60_000 } }.to_yaml)
ENV["PROTOBUF_NATS_CONFIG_PATH"] = config_path

def mono; ::Process.clock_gettime(::Process::CLOCK_MONOTONIC); end

def start_nats(port)
  pid = ::Process.spawn("nats-server", "-p", port.to_s, [:out, :err] => "/dev/null")
  # Wait for the port to accept connections.
  deadline = mono + 10
  loop do
    begin
      ::TCPSocket.new("127.0.0.1", port).close
      break
    rescue
      raise "nats-server did not start" if mono > deadline
      sleep 0.05
    end
  end
  pid
end

require "socket"
require "securerandom"
require "concurrent"

nats_pid = start_nats(PORT)
puts "[soak] nats-server pid=#{nats_pid} on :#{PORT}  duration=#{DURATION}s threads=#{THREADS} bounces=#{BOUNCES}"

require "./examples/warehouse/app"
::Protobuf::Logging.logger = ::Logger.new(nil)

# --- Observe the resilience signals ---
counters = ::Concurrent::Map.new
%w[client.request_timeout client.request_nack server.message_dropped
   server.handler_overdue server.thread_pool_saturated].each do |evt|
  counters[evt] = ::Concurrent::AtomicFixnum.new(0)
  ::ActiveSupport::Notifications.subscribe("#{evt}.protobuf-nats") { counters[evt].increment }
end
# --- Start the server in a background thread ---
server = ::Protobuf::Nats::Server.new(:threads => 10)
server_thread = ::Thread.new { server.run }
sleep 1 # let it subscribe / slow-start a round

# --- Drive load ---
ok = ::Concurrent::AtomicFixnum.new(0)
err = ::Concurrent::AtomicFixnum.new(0)
stop = ::Concurrent::AtomicBoolean.new(false)

workers = THREADS.times.map do
  ::Thread.new do
    until stop.true?
      begin
        # Mostly fast; ~10% deliberately long handlers (allowed, not aborted).
        sleep_ms = (rand(10).zero? ? 300 : 0)
        req = ::Warehouse::Shipment.new(:guid => ::SecureRandom.uuid, :sleep_time_ms => sleep_ms)
        ::Warehouse::ShipmentService.client.create(req)
        ok.increment
      rescue => _e
        err.increment
      end
    end
  end
end

# --- Chaos: bounce nats-server a few times during the run ---
chaos = ::Thread.new do
  interval = DURATION.to_f / (BOUNCES + 1)
  BOUNCES.times do |i|
    sleep interval
    puts "[soak] chaos bounce #{i + 1}/#{BOUNCES}: killing nats-server"
    ::Process.kill("KILL", nats_pid) rescue nil
    ::Process.wait(nats_pid) rescue nil
    sleep 0.5
    nats_pid = start_nats(PORT)
    puts "[soak] nats-server restarted pid=#{nats_pid}"
  end
end

sleep DURATION
stop.make_true
workers.each(&:join)
chaos.join

server.stop
server_thread.join(15)

total = ok.value + err.value
rate = total.zero? ? 0.0 : (ok.value.to_f / total * 100)

puts
puts "================ soak results ================"
puts "duration=#{DURATION}s threads=#{THREADS} nats bounces=#{BOUNCES}"
printf "requests: %d ok, %d failed (%.2f%% success)\n", ok.value, err.value, rate
puts "observed signals:"
counters.each_pair { |evt, c| printf("  %-32s %d\n", evt, c.value) }
puts "=============================================="

# After chaos settles, the system should recover: most requests succeed.
if rate >= 90.0
  puts "[soak] PASS (recovered through #{BOUNCES} nats bounces)"
  status = 0
else
  puts "[soak] FAIL (success rate #{rate.round(2)}% < 90%)"
  status = 1
end

::Process.kill("KILL", nats_pid) rescue nil
::Process.wait(nats_pid) rescue nil
exit status
