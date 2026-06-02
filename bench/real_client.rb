ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"

require "benchmark/ips"
require "./examples/warehouse/app"

Protobuf::Logging.logger = ::Logger.new(nil)

Benchmark.ips do |config|
  config.warmup = 15
  config.time = 30

  config.report("single threaded performance") do
    req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
    Warehouse::ShipmentService.client.create(req)
  end
end
