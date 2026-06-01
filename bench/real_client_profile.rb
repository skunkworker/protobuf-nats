# ENV["PB_CLIENT_TYPE"] = "protobuf/nats/client"
# ENV["PB_SERVER_TYPE"] = "protobuf/nats/runner"

# require "benchmark/ips"
# require "./examples/warehouse/app"

# # require "jruby/profiler/flame_graph_profile_printer"

# Protobuf::Logging.logger = ::Logger.new(nil)

# result = JRuby::Profiler.profile do
#   20_000.times do
#     req = Warehouse::Shipment.new(:guid => SecureRandom.uuid)
#     Warehouse::ShipmentService.client.create(req)
#   end
# end

# printer = JRuby::Profiler::FlameGraphProfilePrinter.new(result)
# stdout = File.open('real_client.out', 'w')
# printer.printProfile(stdout)
# stdout.close