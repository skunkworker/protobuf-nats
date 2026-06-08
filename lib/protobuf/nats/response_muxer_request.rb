require 'securerandom'
require "connection_pool"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

module Protobuf
  module Nats
    class ResponseMuxerRequest
      def initialize(muxer, token)
        @muxer = muxer
        @token = token
      end

      def publish(subject, data)
        @muxer.publish(subject, data, @token)
      end

      def next_message(timeout)
        @muxer.next_message(@token, timeout)
      end

      def cleanup
        @muxer.cleanup(@token)
      end
    end
  end
end
