# frozen_string_literal: true

source 'https://rubygems.org'

# Specify your gem's dependencies in activerecord-duckdb.gemspec
gemspec

gem 'irb'
gem 'rake', '~> 13.0'

group :development do
  gem 'fasterer'
  gem 'rubocop', '~> 1.76', require: false
  gem 'rubocop-performance', '~> 1.25', require: false
  gem 'rubocop-rake', '~> 0.7', require: false
  gem 'rubocop-rspec', '~> 3.6', require: false
  gem 'rubocop-thread_safety', require: false
  gem 'sord'
end

group :test do
  gem 'fuubar'
  gem 'rspec', '~> 3.13'
  gem 'simplecov', require: false
  gem 'simplecov-tailwindcss', require: false
end
