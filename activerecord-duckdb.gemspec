# frozen_string_literal: true

require_relative 'lib/activerecord/duckdb/version'

Gem::Specification.new do |spec|
  spec.name = 'activerecord-duckdb'
  spec.version = Activerecord::Duckdb::VERSION
  spec.authors = ['Brandon Hicks']
  spec.email = ['tarellel@gmail.com']

  spec.summary = 'A DuckDB database adapter for ActiveRecord.'
  spec.description = 'Activerecord::Duckdb providers DuckDB database access for Ruby on Rails applications.'
  spec.homepage = 'https://github.com/tarellel/activerecord-duckdb'
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 3.1.0'

  # spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir = 'exe'
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"
  spec.add_dependency 'activerecord', '>= 7.0.0'
  spec.add_dependency 'duckdb', '>= 1.3', '< 2.0'
  spec.metadata['rubygems_mfa_required'] = 'true'
end
