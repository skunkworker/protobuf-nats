require "protobuf/nats"

require 'protobuf/message'
require 'protobuf/rpc/service'

module Warehouse

  ##
  # Message Classes
  #
  class Shipment < ::Protobuf::Message; end
  class ShipmentRequest < ::Protobuf::Message; end
  class Shipments < ::Protobuf::Message; end

  ##
  # Message Fields
  #
  class Shipment
    optional :string, :guid, 1
    optional :string, :address, 2
    optional :double, :price, 3
    optional :string, :package_guid, 4
  end

  class ShipmentRequest
    repeated :string, :guid, 1
    repeated :string, :address, 2
    repeated :string, :package_guid, 3
  end

  class Shipments
    repeated ::Warehouse::Shipment, :records, 1
  end

  ##
  # Service Classes
  #
  class ShipmentService < ::Protobuf::Rpc::Service
    rpc :create, ::Warehouse::Shipment, ::Warehouse::Shipment
    rpc :not_implemented, ::Warehouse::Shipment, ::Warehouse::Shipment
    rpc :search, ::Warehouse::ShipmentRequest, ::Warehouse::Shipments

    def create
      respond_with request
    end

    def search
      shipment = ::Warehouse::Shipment.new(:guid => SecureRandom.uuid, :address => "123 LAME ST", :price => 100.0, :package_guid => SecureRandom.uuid)
      respond_with ::Warehouse::Shipments.new(:records => [shipment])
    end
  end


  ##
  # Message Classes
  #
  class CargoShip < ::Protobuf::Message; end
  class CargoShipRequest < ::Protobuf::Message; end
  class CargoShips < ::Protobuf::Message; end

  ##
  # Message Fields
  #
  class CargoShip
    optional :string, :name, 1
    optional :string, :guid, 2
    optional :string, :status, 3
  end

  class CargoShips
    repeated ::Warehouse::CargoShip, :records, 1
  end

  class CargoShipRequest
    repeated :string, :name, 1
    repeated :string, :guid, 2
    repeated :string, :status, 3
  end

  class ShipService < ::Protobuf::Rpc::Service
    rpc :create, ::Warehouse::CargoShip, ::Warehouse::CargoShip
    rpc :search, ::Warehouse::CargoShipRequest, ::Warehouse::CargoShip

    def create
      respond_with request
    end

    def search
      ship = ::Warehouse::CargoShip.new(:guid => SecureRandom.uuid, :name => SecureRandom.uuid, :status => SecureRandom.uuid)
      respond_with ::Warehouse::CargoShip.new(:records => [ship])
    end
  end

end


# TODO: add multiple rounds of subscriptions server side in order to test 1 receiver thread to N in the thread_pool