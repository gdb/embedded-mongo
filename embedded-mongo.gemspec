# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "embedded-mongo/version"

Gem::Specification.new do |s|
  s.name        = "embedded-mongo"
  s.version     = EmbeddedMongo::VERSION
  s.authors     = ["Greg Brockman"]
  s.email       = ["gdb@gregbrockman.com"]
  s.homepage    = "https://github.com/gdb/embedded-mongo"
  s.summary     = %q{A Ruby implementation of the MongoDB interface}
  s.description = %q{embedded-mongo's goal is to provide the same interface as mongodb but
  be embedded inside the calling process.  This allows unit tests to be
  run without the overhead of database roundtrips or the creation of
  ad-hoc mock layers.  It also allows one to start using the mongodb
  interface for new projects without having to set up a real database.
  I don't think there's a use-case for it in production, though I could
  be wrong.
  }

  s.rubyforge_project = "embedded-mongo"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  # s.add_development_dependency "rspec"
  # s.add_runtime_dependency "rest-client"
  s.add_runtime_dependency "mongo"
end
