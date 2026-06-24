require 'simplecov'
SimpleCov.start

# Keep the response muxer deterministic in tests: a single dispatcher thread.
# (In production this auto-scales on JRuby; see ResponseMuxer#dispatcher_count.)
ENV["PB_NATS_RESPONSE_MUXER_DISPATCHERS"] ||= "1"

require "bundler/setup"
require "protobuf/nats"
require "fake_nats_client"
require "pry"

# Turn off protobuf logging.
::Protobuf::Logging.logger = ::Logger.new(nil)

# Deterministic polling helper for concurrency specs: wait for a condition
# instead of sleeping a fixed amount and hoping. Fails fast on timeout.
module WaitHelpers
  def wait_until(timeout: 2, interval: 0.005)
    deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + timeout
    until yield
      if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) > deadline
        raise "wait_until timed out after #{timeout}s"
      end
      sleep interval
    end
  end
end

RSpec.configure do |config|
  config.include WaitHelpers

  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"
  config.order = :random
  config.color = true

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do
    allow(::Protobuf::Nats).to receive(:start_client_nats_connection)

    ::Protobuf::Nats::Client::RESPONSE_MUXER.restart
  end
end
