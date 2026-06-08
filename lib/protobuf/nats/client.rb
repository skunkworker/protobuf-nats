require 'securerandom'
require "connection_pool"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

# Load this independently because we store the class singleton in a const.
require "protobuf/nats/response_muxer"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base

      RESPONSE_MUXER = ::Protobuf::Nats::ResponseMuxer.new

      @subscription_key_cache = {}
      @subscription_pool_lock = ::Mutex.new

      # Structure to hold subscription and inbox to use within pool
      SubscriptionInbox = ::Struct.new(:subscription, :inbox) do
        def swap(sub_inbox)
          self.subscription = sub_inbox.subscription
          self.inbox = sub_inbox.inbox
        end
      end

      def logger
        ::Protobuf::Logging.logger
      end

      def response_muxer
        RESPONSE_MUXER
      end

      def self.subscription_pool
        return @subscription_pool if @subscription_pool

        @subscription_pool_lock.synchronize do
          # The double-check ensures we don't create a new pool if another
          # thread created one while we were waiting for the lock.
          return @subscription_pool if @subscription_pool

          @subscription_pool = ::ConnectionPool.new(:size => subscription_pool_size, :timeout => 0.1) do
            inbox = ::Protobuf::Nats.client_nats_connection.new_inbox
            SubscriptionInbox.new(::Protobuf::Nats.client_nats_connection.subscribe(inbox), inbox)
          end
        end
      end

      def self.subscription_pool_size
        @subscription_pool_size ||= if ::ENV.key?("PB_NATS_CLIENT_SUBSCRIPTION_POOL_SIZE")
          ::ENV["PB_NATS_CLIENT_SUBSCRIPTION_POOL_SIZE"].to_i
        else
          0
        end
      end

      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super

        # This will ensure the client is started.
        ::Protobuf::Nats.start_client_nats_connection

        # Ensure the response muxer is started
        RESPONSE_MUXER.start
      end

      def new_subscription_inbox
        nats = ::Protobuf::Nats.client_nats_connection
        inbox = nats.new_inbox
        sub = if use_subscription_pooling?
                nats.subscribe(inbox)
              else
                nats.subscribe(inbox, :max => 2)
              end

        SubscriptionInbox.new(sub, inbox)
      end

      def with_subscription
        return_value = nil

        if use_subscription_pooling?
          self.class.subscription_pool.with do |sub_inbox|
            return_value = yield sub_inbox
          end
        else
          return_value = yield new_subscription_inbox
        end

        return_value
      end

      def close_connection
        # no-op (I think for now), the connection to server is persistent
      end

      def self.subscription_key_cache
        @subscription_key_cache
      end

      def ack_timeout
        @ack_timeout ||= if ::ENV.key?("PB_NATS_CLIENT_ACK_TIMEOUT")
          ::ENV["PB_NATS_CLIENT_ACK_TIMEOUT"].to_i
        else
          5
        end
      end

      def nack_backoff_intervals
        @nack_backoff_intervals ||= if ::ENV.key?("PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS")
          ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS"].split(",").map(&:to_i)
        else
          [0, 1, 3, 5, 10]
        end
      end

      def nack_backoff_splay
        @nack_backoff_splay ||= if nack_backoff_splay_limit > 0
          rand(nack_backoff_splay_limit)
        else
          0
        end
      end

      def nack_backoff_splay_limit
        @nack_backoff_splay_limit ||= if ::ENV.key?("PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT")
          ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT"].to_i
        else
          10
        end
      end

      def reconnect_delay
        @reconnect_delay ||= if ::ENV.key?("PB_NATS_CLIENT_RECONNECT_DELAY")
          ::ENV["PB_NATS_CLIENT_RECONNECT_DELAY"].to_i
        else
          ack_timeout
        end
      end

      def response_timeout
        @response_timeout ||= if ::ENV.key?("PB_NATS_CLIENT_RESPONSE_TIMEOUT")
          ::ENV["PB_NATS_CLIENT_RESPONSE_TIMEOUT"].to_i
        else
          60
        end
      end

      def use_subscription_pooling?
        return @use_subscription_pooling unless @use_subscription_pooling.nil?
        @use_subscription_pooling = self.class.subscription_pool_size > 0
      end

      def send_request
        # This will ensure the client is started.
        ::Protobuf::Nats.start_client_nats_connection

        if use_subscription_pooling?
          available = self.class.subscription_pool.instance_variable_get("@available")
          ::ActiveSupport::Notifications.instrument "client.subscription_pool_available_size.protobuf-nats", available.length
        end

        ::ActiveSupport::Notifications.instrument "client.request_duration.protobuf-nats" do
          send_request_through_nats
        end
      end

      def send_request_through_nats
        retries ||= 3
        nack_retry ||= 0

        loop do
          setup_connection
          request_options = {:timeout => response_timeout, :ack_timeout => ack_timeout}
          @response_data = nats_request_with_two_responses(cached_subscription_key, @request_data, request_options)
          case @response_data
          when :ack_timeout
            ::ActiveSupport::Notifications.instrument "client.request_timeout.protobuf-nats"
            next if (retries -= 1) > 0
            raise ::Protobuf::Nats::Errors::RequestTimeout, formatted_service_and_method_name
          when :nack
            ::ActiveSupport::Notifications.instrument "client.request_nack.protobuf-nats"
            interval = nack_backoff_intervals[nack_retry]
            nack_retry += 1
            raise ::Protobuf::Nats::Errors::RequestTimeout, formatted_service_and_method_name if interval.nil?
            sleep((interval + nack_backoff_splay)/1000.0)
            next
          end

          break
        end

        parse_response
      rescue ::Protobuf::Nats::Errors::IOException => error
        ::Protobuf::Nats.log_error(error)

        delay = reconnect_delay
        logger.warn "An IOException was raised. We are going to sleep for #{delay} seconds."
        sleep delay

        retry if (retries -= 1) > 0
        raise
      end

      def cached_subscription_key
        klass = @options[:service]
        method_name = @options[:method]

        method_name_cache = self.class.subscription_key_cache[klass] ||= {}
        method_name_cache[method_name] ||= begin
          ::Protobuf::Nats.subscription_key(klass, method_name)
        end
      end

      def formatted_service_and_method_name
        klass = @options[:service]
        method_name = @options[:method]
        "#{klass}##{method_name}"
      end

      def nats_request_with_two_responses(subject, data, opts)
        # Wait for the ACK from the server
        ack_timeout = opts[:ack_timeout] || 5
        # Wait for the protobuf response
        timeout = opts[:timeout] || 60

        nats = Protobuf::Nats.client_nats_connection

        # Publish message with the reply topic pointed at the response muxer.
        req = RESPONSE_MUXER.new_request
        req.publish(subject, data)

        # Receive the first message
        begin
          first_message = req.next_message(ack_timeout)
          logger.debug "received message with subject:#{first_message.subject}"
        rescue ::NATS::Timeout => e
          return :ack_timeout
        end

        # Check for a NACK
        return :nack if first_message.data == ::Protobuf::Nats::Messages::NACK

        # Receive the second message
        begin
          second_message = req.next_message(timeout)
        rescue ::NATS::Timeout
          # ignore to raise a repsonse timeout below
        end

        # NOTE: This might be nil, so be careful checking the data value
        second_message_data = second_message&.data

        # This should never happen, if it does, then return an :ack_timeout because something went wrong
        if first_message&.data == ::Protobuf::Nats::Messages::ACK &&
          second_message&.data == ::Protobuf::Nats::Messages::ACK
          logger.warn "received ACK/ACK message."
          return :ack_timeout
        end

        # Check messages
        response = case ::Protobuf::Nats::Messages::ACK
                   when first_message&.data then second_message_data
                   when second_message&.data then first_message&.data
                   else return :ack_timeout
                   end

        fail(::Protobuf::Nats::Errors::ResponseTimeout, formatted_service_and_method_name) unless response

        response
      ensure
        # cleanup the token from the request map
        req.cleanup if req
      end

    end
  end
end
