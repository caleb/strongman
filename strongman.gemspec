$LOAD_PATH.push File.expand_path('../lib', __FILE__)

require 'strongman/version'

Gem::Specification.new do |s|
  s.name        = 'strongman'
  s.version     = Strongman::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Caleb Land']
  s.email       = ['caleb@land.fm']
  s.homepage    = 'https://github.com/caleb/strongman'
  s.summary     = 'Batch data loading, works great with graphql'
  s.description = 'A data loading utility to batch loading of promises. It can be used with graphql gem.'
  s.license     = 'MIT'

  s.add_runtime_dependency("concurrent-ruby", "~> 1.1")

  s.files         = Dir['lib/**/*'] + %w(LICENSE README.md)
  s.require_paths = ['lib']
end
