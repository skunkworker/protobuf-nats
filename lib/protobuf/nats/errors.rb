module Protobuf
  module Nats
    module Errors
      class ClientError < ::StandardError
      end

      class RequestTimeout < ClientError
      end

      class ResponseTimeout < ClientError
      end

      class MriIOException < ::StandardError
      end

      IOException = MriIOException
    end
  end
end
