require 'securerandom'
require "concurrent"
require "protobuf/nats"
require "protobuf/rpc/connectors/base"
require "monitor"

# Required here: we store the class singleton in a const below.
require "protobuf/nats/response_muxer"

module Protobuf
  module Nats
    class Client < ::Protobuf::Rpc::Connectors::Base

      RESPONSE_MUXER = ::Protobuf::Nats::ResponseMuxer.new

      # On JRuby, concurrent writes to a plain Hash can raise
      # ConcurrentModificationError, so the cache must be a Concurrent::Map.
      # On CRuby the GVL makes plain-Hash access atomic, and a plain Hash
      # is faster, so we keep the Hash there.
      CONCURRENT_SUBSCRIPTION_CACHE = (::RUBY_ENGINE == "jruby")

      @subscription_key_cache = CONCURRENT_SUBSCRIPTION_CACHE ? ::Concurrent::Map.new : {}

      def logger
        ::Protobuf::Logging.logger
      end

      def response_muxer
        RESPONSE_MUXER
      end

      def initialize(options)
        super

        ::Protobuf::Nats.start_client_nats_connection
        RESPONSE_MUXER.start
      end

      def close_connection
        # No-op. The connection to the server stays open and is shared.
      end

      def self.subscription_key_cache
        @subscription_key_cache
      end

      def ack_timeout
        @ack_timeout ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_ACK_TIMEOUT", 5)
      end

      # Milliseconds between NACK retries. A NACK means the server's pool is
      # full. The 2017 default ([0, 1, 3, 5, 10]) sent six attempts to a
      # saturated queue group in about 64ms, which added load when the
      # server had none to spare. This one spreads them over about 1.6s.
      DEFAULT_NACK_BACKOFF_INTERVALS = [0, 50, 150, 400, 1000].freeze

      def nack_backoff_intervals
        @nack_backoff_intervals ||= begin
          raw = ::ENV["PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS"]
          if raw.nil?
            DEFAULT_NACK_BACKOFF_INTERVALS
          else
            # Parse strictly, like env_int. "fast,slow".to_i would silently
            # give [0, 0] (no backoff) instead of the default.
            begin
              raw.split(",").map { |interval| Integer(interval.strip, 10) }
            rescue ::ArgumentError
              logger.error "Ignoring malformed interval list in ENV PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS=#{raw.inspect}; using default #{DEFAULT_NACK_BACKOFF_INTERVALS.inspect}"
              DEFAULT_NACK_BACKOFF_INTERVALS
            end
          end
        end
      end

      # Chosen again for each retry, not once per client object.
      def nack_backoff_splay
        nack_backoff_splay_limit > 0 ? rand(nack_backoff_splay_limit) : 0
      end

      def nack_backoff_splay_limit
        @nack_backoff_splay_limit ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT", 10)
      end

      def reconnect_delay
        @reconnect_delay ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_RECONNECT_DELAY", ack_timeout)
      end

      # Random jitter (seconds) added to reconnect_delay, so a fleet does
      # not reconnect in lockstep after a shared outage. Limit is in ms.
      def reconnect_delay_splay
        return 0 unless reconnect_delay_splay_limit > 0
        rand(reconnect_delay_splay_limit) / 1000.0
      end

      def reconnect_delay_splay_limit
        @reconnect_delay_splay_limit ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT", 1000)
      end

      # Retry count for ack-timeouts and transient transport errors.
      def max_retries
        @max_retries ||= ::Protobuf::Nats.env_int("PB_NATS_CLIENT_MAX_RETRIES", 3, :min => 1)
      end

      def response_timeout
        @response_timeout ||= ::Protobuf::Nats.client_response_timeout
      end

      def send_request
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
          when :no_responders
            ::Protobuf::Nats.instrument "client.no_responders"
            raise ::Protobuf::Nats::Errors::NoResponders, formatted_service_and_method_name unless (retries -= 1) > 0
            # No server subscribes now (a deploy, a pause file, an outage).
            # Wait like a transport retry, so a new server has time to
            # subscribe. Do not retry at once: the 503 comes back in
            # milliseconds, so all attempts would fail in one burst.
            sleep(reconnect_delay + reconnect_delay_splay)
            next
          end

          break
        end

        parse_response
      rescue *::Protobuf::Nats::Errors::RETRYABLE_TRANSPORT_ERRORS => error
        ::Protobuf::Nats.log_error(error)

        if (retries -= 1) > 0
          # Only sleep when a retry follows. Sleeping before the final
          # raise would just delay the failure for nothing.
          delay = reconnect_delay + reconnect_delay_splay
          logger.warn "A transient transport error was raised (#{error.class}). Sleeping #{delay.round(3)}s before retrying."
          sleep delay

          # The connection may be terminally dead (nats-pure gave up
          # reconnecting and dropped the memoized client). Rebuild it, and
          # move the muxer's inbox onto the new connection, before
          # retrying; otherwise the retry publishes into a closed
          # connection and fails the same way. A rebuild failure just
          # uses up this retry, like any other transport error.
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
        # Named first_message_timeout, not ack_timeout, to avoid shadowing
        # the #ack_timeout fallback method used below.
        first_message_timeout = opts[:ack_timeout] || ack_timeout
        response_message_timeout = opts[:timeout] || response_timeout

        req = RESPONSE_MUXER.new_request
        req.publish(subject, data)

        begin
          first_message = req.next_message(first_message_timeout)
          logger.debug { "received message with subject:#{first_message.subject}" } if logger.debug?
        rescue ::NATS::Timeout => e
          return :ack_timeout
        end

        return :nack if first_message.data == ::Protobuf::Nats::Messages::NACK

        # nats-pure sends `no_responders: true` in CONNECT, so a request to
        # a subject with no subscriber gets an empty 503 status message at
        # once. It is not an ACK: do not wait response_timeout for a second
        # message that never comes (it turned a 15s failure into 180s).
        return :no_responders if no_responders?(first_message)

        begin
          second_message = req.next_message(response_message_timeout)
        rescue ::NATS::Timeout
          # Ignore. This raises a response timeout below instead.
        end

        # May be nil: check the data value carefully below.
        second_message_data = second_message&.data

        # Two ACKs should never happen. Treat it as a timeout if it does.
        if first_message&.data == ::Protobuf::Nats::Messages::ACK &&
          second_message&.data == ::Protobuf::Nats::Messages::ACK
          logger.warn "received ACK/ACK message."
          return :ack_timeout
        end

        response = case ::Protobuf::Nats::Messages::ACK
                   when first_message&.data then second_message_data
                   when second_message&.data then first_message&.data
                   else return :ack_timeout
                   end

        fail(::Protobuf::Nats::Errors::ResponseTimeout, formatted_service_and_method_name) unless response

        response
      ensure
        # Remove the token from the request map.
        req.cleanup if req
      end

      # Status header value nats-server uses for "no responders".
      NO_RESPONDERS_STATUS = "503".freeze

      def no_responders?(message)
        header = message.header
        !header.nil? && header[::NATS::IO::Client::STATUS_HDR] == NO_RESPONDERS_STATUS
      end

    end
  end
end
