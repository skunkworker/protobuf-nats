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

      # Raised into a worker thread to reclaim a handler that has outlived the
      # client's response_timeout. Only used when overdue-reclaim is explicitly
      # enabled via PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS (default off); the
      # documented default is that handlers are never aborted.
      class HandlerOverdue < ::StandardError
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
        # Raised when a request races a ResponseMuxer restart (its inbox prefix
        # is briefly nil while it rebuilds on a new connection). Transient by
        # nature: the next attempt runs after the muxer has restarted.
        ResponseMuxer,
        ::EOFError,
        ::IOError,
        ::Errno::ECONNRESET,
        ::Errno::ECONNREFUSED,
        ::Errno::ECONNABORTED,
        ::Errno::EPIPE,
        ::Errno::ETIMEDOUT,
        # Raised when a NATS node (or the route to it) dies without sending a
        # FIN/RST -- e.g. a network partition or a hard host failure. nats-pure
        # fails over to another node in the pool; ride it out and retry.
        ::Errno::EHOSTUNREACH,
        ::Errno::ENETUNREACH,
      ].tap do |errors|
        # nats-pure raises this when publishing on a closed connection.
        errors << ::NATS::IO::ConnectionClosedError if defined?(::NATS::IO::ConnectionClosedError)
        # On JRuby, socket EOF can still surface as a Java IOException.
        errors << ::Java::JavaIo::IOException if defined?(::JRUBY_VERSION)
      end.freeze
    end
  end
end
