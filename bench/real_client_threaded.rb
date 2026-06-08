ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"

require "./examples/warehouse/app"

THREAD_COUNT = ENV.fetch("CLIENT_THREADS",4).to_i

puts "THREAD_COUNT = #{THREAD_COUNT}"

::Protobuf::Logging.logger = ::Logger.new(nil)

while true
  THREAD_COUNT.times.map do |i|
    Thread.new do
      req = Warehouse::Shipment.new(:guid => SecureRandom.uuid, :sleep_time_ms => 100)
      Warehouse::ShipmentService.client.create(req)
    end
  end.each(&:join)
end
