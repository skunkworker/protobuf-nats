# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'protobuf/nats/version'

Gem::Specification.new do |spec|
  spec.name          = "protobuf-nats"
  spec.version       = Protobuf::Nats::VERSION
  spec.authors       = ["Brandon Dewitt"]
  spec.email         = ["brandonsdewitt@gmail.com"]

  spec.summary       = %q{ ruby-protobuf client/server for nats }
  spec.description   = %q{ ruby-protobuf client/server for nats }
  spec.homepage      = "https://github.com/mxenabled/protobuf-nats"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "https://rubygems.org"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end

  spec.required_ruby_version = '>= 3.1.0'

  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "activesupport", ">= 6.1"
  spec.add_runtime_dependency "concurrent-ruby", "~> 1.3.6" # pinned so logger is included
  spec.add_runtime_dependency "protobuf", "~> 3.7", ">= 3.7.2"
  # Floor at 2.5: this gem reaches into nats-pure internals that are not
  # public API (the subscription pending_queue swap, pending_msgs_limit drop
  # semantics, subscription replay on reconnect, max_reconnect_attempts < 0 ==
  # infinite), all verified against 2.5. Re-verify those before widening.
  spec.add_runtime_dependency "nats-pure", ">= 2.5", "< 3"

  spec.add_dependency "uuid7" # Remove once on newer ruby versions which include this in PRNG.

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "benchmark-ips"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "simplecov"
end
