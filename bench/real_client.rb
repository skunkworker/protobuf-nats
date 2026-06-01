ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"

require "benchmark/ips"
require "./examples/warehouse/app"

# require "jruby/profiler/flame_graph_profile_printer"

Protobuf::Logging.logger = ::Logger.new(nil)

# should_profile = ENV["profile"]

Benchmark.ips do |config|
  config.warmup = 15
  config.time = 30

  config.report("single threaded performance") do
    req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
    Warehouse::ShipmentService.client.create(req)
  end
end


# export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m --dev --debug --profile"
#
# export JRUBY_OPTS="--disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom -J-Xmx2g -J-Xms1024m -J-Xmn512m"
#
