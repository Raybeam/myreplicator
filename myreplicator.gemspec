$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "myreplicator/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "myreplicator"
  s.version     = Myreplicator::VERSION
  s.authors     = ["Sasan Padidar"]
  s.email       = ["sasan@raybeam.com"]
  s.homepage    = "www.raybea.com"
  s.summary     = "Simpler way to do replication instead of using mysql's replication."
  s.description = "TODO: Description of Myreplicator."

  s.files = Dir["{app,config,db,lib}/**/*"] + ["MIT-LICENSE", "Rakefile", "README.rdoc"]
  s.test_files = Dir["test/**/*"]

  s.add_dependency "rails", "~> 3.2.8"
  # s.add_dependency "jquery-rails"

  s.add_development_dependency "sqlite3"
end
