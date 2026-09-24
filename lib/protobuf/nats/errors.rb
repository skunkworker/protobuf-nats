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

      # Raised by ResponseMuxer#start when the response subscription lacks
      # #synchronize, needed for pending_size byte accounting. nats-pure's
      # Subscription always includes MonitorMixin, so this is a tripwire
      # for a nats-pure internals change or a non-standard client, not a
      # real-world case. Without #synchronize, pending_size would only
      # grow and eventually false-trip the byte cap, silently dropping
      # every response. Fail loudly at start instead.
      #
      # Not in RETRYABLE_TRANSPORT_ERRORS: retrying cannot fix a structural
      # mismatch. Left out of the README as an internal tripwire.
      class IncompatibleSubscription < ClientError
      end

      class MriIOException < ::StandardError
      end

      # Raised into a worker thread to reclaim a handler that outlived the
      # client's response_timeout. Only used when
      # PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS is on (default off).
      class HandlerOverdue < ::StandardError
      end

      IOException = MriIOException

      # Transient transport errors: the connection is unavailable or was
      # dropped mid-request. Sleep for reconnect_delay and retry (nats-pure
      # reconnects in the background) instead of raising RPC_ERROR.
      #
      # NOTE: removing jnats collapsed IOException to MriIOException, which
      # nothing raises, silently disabling reconnect/retry. This list
      # restores it with the errors the pure-ruby client actually raises.
      RETRYABLE_TRANSPORT_ERRORS = [
        IOException, # legacy / explicit wraps
        # Raised when a request races a ResponseMuxer restart (its inbox
        # prefix is briefly nil while rebuilding). The next attempt runs
        # after the muxer restarts.
        ResponseMuxer,
        ::EOFError,
        ::IOError,
        ::Errno::ECONNRESET,
        ::Errno::ECONNREFUSED,
        ::Errno::ECONNABORTED,
        ::Errno::EPIPE,
        ::Errno::ETIMEDOUT,
        # Raised when a NATS node (or its route) dies without a FIN/RST,
        # e.g. a network partition or hard host failure. nats-pure fails
        # over to another node; ride it out and retry.
        ::Errno::EHOSTUNREACH,
        ::Errno::ENETUNREACH,
      ].tap do |errors|
        # nats-pure raises this on a publish to a closed connection.
        errors << ::NATS::IO::ConnectionClosedError if defined?(::NATS::IO::ConnectionClosedError)
        # On JRuby, socket EOF can surface as a Java IOException.
        errors << ::Java::JavaIo::IOException if defined?(::JRUBY_VERSION)
      end.freeze
    end
  end
end
