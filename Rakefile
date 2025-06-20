# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rubocop/rake_task'
begin
  require 'rspec/core/rake_task'

  RSpec::Core::RakeTask.new(:spec) do |t|
    t.pattern = Dir.glob('spec/**/*_spec.rb')
    t.rspec_opts = '--format documentation'
  end
rescue LoadError
  # If RSpec is not available, handle appropriately
  puts 'RSpec gem not found. Please add it to your Gemfile.'
end

RuboCop::RakeTask.new

task default: %i[rubocop spec]
