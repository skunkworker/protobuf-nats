require "openssl"
require "nats/io/client"

module Protobuf
  module Nats
    # Make NATS::IO::Socket honor SSLContext#verify_hostname on every Ruby.
    #
    # nats-pure sets the SSLSocket hostname to the server it connects to
    # (each server's own on a reconnect), so on CRuby verify_hostname alone
    # rejects a wrong cert in the handshake. jruby-openssl (0.15.4, JRuby 9.4
    # and 10) accepts the setting but does not check it: a cert for another
    # host connects. post_connection_check works on both, so run it after the
    # handshake. On CRuby it repeats a check that already passed.
    module TlsHostnameCheck
      def setup_tls!
        super
        return unless @tls.fetch(:context).verify_hostname

        hostname = @tls[:hostname]
        # Fail closed: without a hostname there is nothing to check against.
        raise ::OpenSSL::SSL::SSLError, "TLS hostname verification is on, but nats-pure gave no hostname" if hostname.nil? || hostname.empty?
        @socket.post_connection_check(hostname)
      end
    end
  end
end

::NATS::IO::Socket.prepend(::Protobuf::Nats::TlsHostnameCheck)
