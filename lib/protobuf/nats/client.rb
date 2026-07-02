require 'securerandom'
require "concurrent"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

# Load this independently because we store the class singleton in a const.
require "protobuf/nats/response_muxer"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base

      RESPONSE_MUXER = ::Protobuf::Nats::ResponseMuxer.new

      # On JRuby (true parallelism) concurrent writes to a plain nested Hash can
      # raise ConcurrentModificationError / corrupt the map, so the cache must be
      # a Concurrent::Map. On CRuby the GVL makes plain-Hash reads/writes atomic
      # (a racing `||=` at worst recomputes an identical value), and a plain Hash
      # is meaningfully faster than Concurrent::Map, so we keep the Hash there.
      CONCURRENT_SUBSCRIPTION_CACHE = (::RUBY_ENGINE == "jruby")

      @subscription_key_cache = CONCURRENT_SUBSCRIPTION_CACHE ? ::Concurrent::Map.new : {}

      def logger
        ::Protobuf::Logging.logger
      end

      def response_muxer
        RESPONSE_MUXER
      end

      def initialize(options)
        # may need to override to setup connection at this stage ... may also do on load of class
        super

        # This will ensure the client is started.
        ::Protobuf::Nats.start_client_nats_connection

        # Ensure the response muxer is started
        RESPONSE_MUXER.start
      end

      def close_connection
        # no-op (I think for now), the connection to server is persistent
      end

      def self.subscription_key_cache
        @subscription_key_cache
      end

      def ack_timeout
        @ack_timeout ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_ACK_TIMEOUT", 5)
      end

      DEFAULT_NACK_BACKOFF_INTERVALS = [0, 1, 3, 5, 10].freeze

      def nack_backoff_intervals
        @nack_backoff_intervals ||= begin
          raw = ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS"]
          if raw.nil?
            DEFAULT_NACK_BACKOFF_INTERVALS
          else
            # Strict parse, matching env_int: "fast,slow".to_i would silently
            # become [0, 0] (retry with no backoff) instead of the default.
            begin
              raw.split(",").map { |interval| Integer(interval.strip, 10) }
            rescue ::ArgumentError
              logger.error "Ignoring malformed interval list in ENV PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS=#{raw.inspect}; using default #{DEFAULT_NACK_BACKOFF_INTERVALS.inspect}"
              DEFAULT_NACK_BACKOFF_INTERVALS
            end
          end
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
        @nack_backoff_splay_limit ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT", 10)
      end

      def reconnect_delay
        @reconnect_delay ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_RECONNECT_DELAY", ack_timeout)
      end

      # Random jitter (seconds) added to reconnect_delay so a fleet hitting the
      # same NATS outage doesn't reconnect in lockstep. Limit is in milliseconds.
      def reconnect_delay_splay
        return 0 unless reconnect_delay_splay_limit > 0
        rand(reconnect_delay_splay_limit) / 1000.0
      end

      def reconnect_delay_splay_limit
        @reconnect_delay_splay_limit ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT", 1000)
      end

      # Number of attempts for ack-timeouts and transient transport errors.
      def max_retries
        @max_retries ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_MAX_RETRIES", 3, :min => 1)
      end

      def response_timeout
        @response_timeout ||= ::Protobuf::Nats.client_response_timeout
      end

      def send_request
        # This will ensure the client is started.
        ::Protobuf::Nats.start_client_nats_connection

        ::Protobuf::Nats.instrument "client.request_duration" do
          send_request_through_nats
        end
      end

      def send_request_through_nats
        retries ||= max_retries
        nack_retry ||= 0

        loop do
          setup_connection
          request_options = {:timeout => response_timeout, :ack_timeout => ack_timeout}
          @response_data = nats_request_with_two_responses(cached_subscription_key, @request_data, request_options)
          case @response_data
          when :ack_timeout
            ::Protobuf::Nats.instrument "client.request_timeout"
            next if (retries -= 1) > 0
            raise ::Protobuf::Nats::Errors::RequestTimeout, formatted_service_and_method_name
          when :nack
            ::Protobuf::Nats.instrument "client.request_nack"
            interval = nack_backoff_intervals[nack_retry]
            nack_retry += 1
            raise ::Protobuf::Nats::Errors::RequestTimeout, formatted_service_and_method_name if interval.nil?
            sleep((interval + nack_backoff_splay)/1000.0)
            next
          end

          break
        end

        parse_response
      rescue *::Protobuf::Nats::Errors::RETRYABLE_TRANSPORT_ERRORS => error
        ::Protobuf::Nats.log_error(error)

        if (retries -= 1) > 0
          # Only sleep when there is a retry to wait for -- sleeping before the
          # raise on the final attempt just delayed the failure by
          # reconnect_delay for nothing.
          delay = reconnect_delay + reconnect_delay_splay
          logger.warn "A transient transport error was raised (#{error.class}). Sleeping #{delay.round(3)}s before retrying."
          sleep delay

          # The connection object may be terminally dead (nats-pure exhausted its
          # reconnect attempts, fired on_close, and the memoized client was
          # dropped). Rebuild it -- and move the muxer's inbox subscription onto
          # the new connection -- before retrying; otherwise the retry would
          # publish into a nil/closed connection and fail identically. A rebuild
          # failure (all nodes still down) just consumes this retry attempt like
          # any other transport error.
          begin
            ::Protobuf::Nats.start_client_nats_connection
            response_muxer.start
          rescue => reconnect_error
            ::Protobuf::Nats.log_error(reconnect_error)
          end
          retry
        end
        raise
      end

      def cached_subscription_key
        klass = @options[:service]
        method_name = @options[:method]

        cache = self.class.subscription_key_cache
        if CONCURRENT_SUBSCRIPTION_CACHE
          method_name_cache = cache.compute_if_absent(klass) { ::Concurrent::Map.new }
          method_name_cache.compute_if_absent(method_name) do
            ::Protobuf::Nats.subscription_key(klass, method_name)
          end
        else
          method_name_cache = cache[klass] ||= {}
          method_name_cache[method_name] ||= ::Protobuf::Nats.subscription_key(klass, method_name)
        end
      end

      def formatted_service_and_method_name
        klass = @options[:service]
        method_name = @options[:method]
        "#{klass}##{method_name}"
      end

      def nats_request_with_two_responses(subject, data, opts)
        # Wait for the ACK from the server. (Named to avoid shadowing the
        # instance methods used as fallbacks.)
        first_message_timeout = opts[:ack_timeout] || ack_timeout
        # Wait for the protobuf response
        response_message_timeout = opts[:timeout] || response_timeout

        # Publish message with the reply topic pointed at the response muxer.
        req = RESPONSE_MUXER.new_request
        req.publish(subject, data)

        # Receive the first message
        begin
          first_message = req.next_message(first_message_timeout)
          logger.debug { "received message with subject:#{first_message.subject}" } if logger.debug?
        rescue ::NATS::Timeout => e
          return :ack_timeout
        end

        # Check for a NACK
        return :nack if first_message.data == ::Protobuf::Nats::Messages::NACK

        # Receive the second message
        begin
          second_message = req.next_message(response_message_timeout)
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
