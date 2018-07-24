lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "medusa_storage/version"

Gem::Specification.new do |spec|
  spec.name          = "medusa_storage"
  spec.version       = MedusaStorage::VERSION
  spec.authors       = ["Howard Ding"]
  spec.email         = ["hding2@illinois.edu"]

  spec.summary       = %q{Paper over different types of storage}
  spec.description   = %q{Help adapt medusa projects to different kinds of storage, isolating diffences in this code.}
  spec.homepage      = "https://github.com/medusa-project/medusa_storage"
  spec.license       = "University of Illinois/NCSA Open Source License"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", "~> 10.0"

  spec.add_runtime_dependency 'aws-sdk-s3'
  spec.add_runtime_dependency 'parallel', '~> 1.12'
  spec.add_runtime_dependency 'hex_string'
end
