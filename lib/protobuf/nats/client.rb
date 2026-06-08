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

    class ResponseMuxer
      LOCK = ::Mutex.new

      def initialize
        @resp_map = Hash.new { |h,k| h[k] = { } }
        @resp_handlers = []
      end

      def logger
        ::Protobuf::Logging.logger
      end

      def cleanup(token)
        @resp_sub.synchronize { @resp_map.delete(token) }
      end

      def next_message(token, timeout)
        # Calculate the deadline once, up front.
        end_time = Time.now + timeout if timeout

        @resp_sub.synchronize do
          # Loop as long as no message is available.
          while !(@resp_map[token].key?(:response) && !@resp_map[token][:response].empty?)
            # On each loop, calculate the time remaining until the deadline.
            remaining = end_time ? end_time - Time.now : nil

            # If time has run out, we must raise a timeout error. This is the
            # definitive exit condition for the loop.
            raise ::NATS::Timeout if timeout && remaining <= 0

            # Wait only for the time remaining. If the wait is woken up
            # spuriously, the loop repeats, 'remaining' is recalculated
            # (now smaller), and we wait again for the correct shorter duration.
            @resp_map[token][:signal].wait(remaining)
          end

          # This line is only reached if a message was successfully received.
          @resp_map[token][:response].shift
        end
      end

      def new_request
        token = ::SecureRandom.uuid # nats.new_inbox with nuid is not threadsafe.

        @resp_sub.synchronize do
          @resp_map[token][:signal] = @resp_sub.new_cond
        end

        ResponseMuxerRequest.new(self, token)
      end

      def publish(subject, data, token)
        nats = Protobuf::Nats.client_nats_connection
        reply_to = "#{@resp_inbox_prefix}.#{token}"
        nats.publish(subject, data, reply_to)
      end

      def restart
        logger.debug "restarting response_muxer"

        # Stop the existing muxer first, if it's running
        LOCK.synchronize do
          @resp_handlers.each(&:kill)
          @resp_handlers.clear
          @started = false
        end

        # Then start it fresh.
        start
      end

      def start
        return if started?
        LOCK.synchronize do
          # We check this twice in case another thread was waiting for the lock to
          # start this party. Use the unlocked check to prevent deadlocks.
          return if _started?

          nats = ::Protobuf::Nats.client_nats_connection
          return if nats.nil?

          @resp_inbox_prefix = nats.new_inbox

          # Subscribe to our per-instance inbox
          @resp_sub = nats.subscribe("#{@resp_inbox_prefix}.*")
          @started = true
        end

        @resp_handlers << Thread.new do
          Thread.current.name = "response-muxer"
          begin
            loop do
              begin
                # --- Start of per-message block ---
                msg = @resp_sub.pending_queue.pop

                # ACK means the message has been picked up and put into the waiting thread_pool
                next if msg.nil?

                @resp_sub.synchronize do
                  # Decrease pending size since consumed already
                  @resp_sub.pending_size -= msg.data.size

                  # example(random data):
                  # _INBOX.{random_data}.{random_data_msg_id}
                  token = msg.subject.split('.').last

                  logger.debug "token: #{token}, resp_map.keys:#{@resp_map.keys}"

                  unless @resp_map.key?(token)
                    ::ActiveSupport::Notifications.instrument "client.unexpected_message.protobuf-nats", 1

                    logger.warn "Received unexpected message. MSG.subject=#{msg.subject}. RESP_SUBJ.subject=#{@resp_sub.subject}. Dropping unexpected message."

                    # NOTE: use #next instead of a #break here
                    # We want to move onto the next message quickly, rather than escaping from the outer `loop do` loop.
                    next
                  end

                  signal = @resp_map[token][:signal]
                  @resp_map[token][:response] ||= []
                  @resp_map[token][:response] << msg
                  signal.signal
                end
                # --- End of per-message block ---
              rescue => per_message_error
                # Log the error for the specific message, but DON'T kill the thread.
                logger.error("ResponseMuxer failed to process a message. Error: #{per_message_error.message}")
                ::Protobuf::Nats.notify_error_callbacks(per_message_error)
              end
            end
          rescue => fatal_error
            # This block is now only for truly fatal errors that kill the loop itself.
            logger.error("ResponseMuxer thread crashed fatally. Error: #{fatal_error.message}")
            ::Protobuf::Nats.notify_error_callbacks(fatal_error)

            # --- Self-healing logic ---
            @crash_count = (@crash_count || 0) + 1
            # Exponential backoff, e.g., 1, 4, 9, 16s... capped at 60s.
            sleep_duration = [(@crash_count**2), 60].min
            logger.warn("Waiting #{sleep_duration}s before attempting to restart ResponseMuxer.")
            sleep sleep_duration
            # --- End of self-healing logic ---

            # After sleeping, reset the state and try to start again.
            LOCK.synchronize { @started = false }
            start
          end
        end
      end

      def started?
        LOCK.synchronize { _started? }
      end

      private

      def _started?
        !!@started
      end
    end

    class Client < ::Protobuf::Rpc::Connectors::Base

      RESPONSE_MUXER = ResponseMuxer.new

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
