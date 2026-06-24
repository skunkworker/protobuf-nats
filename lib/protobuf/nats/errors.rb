module Protobuf
  module Nats
    module Errors
      class ClientError < ::StandardError
      end

      class RequestTimeout < ClientError
      end

      class ResponseTimeout < ClientError
      end

      class ResponseMuxer < ClientError
      end

      class MriIOException < ::StandardError
      end

      IOException = MriIOException

      # Transient transport errors that mean the NATS connection is unavailable
      # or was dropped mid-request. These should be ridden out by sleeping for
      # reconnect_delay and retrying (nats-pure reconnects in a background
      # thread), rather than bubbling up as an immediate RPC_ERROR.
      #
      # NOTE: when jnats was removed in favor of nats-pure, IOException was
      # collapsed to MriIOException, which nothing ever raises -- silently
      # disabling the client's reconnect/retry path. This list restores it by
      # matching the errors the pure-ruby client and socket layer actually raise.
      RETRYABLE_TRANSPORT_ERRORS = [
        IOException, # legacy / explicit wraps
        ::EOFError,
        ::IOError,
        ::Errno::ECONNRESET,
        ::Errno::ECONNREFUSED,
        ::Errno::EPIPE,
        ::Errno::ETIMEDOUT,
      ].tap do |errors|
        # nats-pure raises this when publishing on a closed connection.
        errors << ::NATS::IO::ConnectionClosedError if defined?(::NATS::IO::ConnectionClosedError)
        # Subscription pool exhaustion during a reconnect is transient too.
        errors << ::ConnectionPool::TimeoutError if defined?(::ConnectionPool::TimeoutError)
        # On JRuby, socket EOF can still surface as a Java IOException.
        errors << ::Java::JavaIo::IOException if defined?(::JRUBY_VERSION)
      end.freeze
    end
  end
end
