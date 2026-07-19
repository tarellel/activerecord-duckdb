# frozen_string_literal: true

require 'active_record/connection_adapters/duckdb/quack_server'

namespace :duckdb do
  namespace :quack do
    desc 'Start a DuckDB quack server so multiple client processes can share one ' \
         'writable database. Env: DATABASE, BIND, QUACK_TOKEN, ' \
         'QUACK_EXTENSIONS (comma-separated), QUACK_ALLOW_OTHER_HOSTNAME=1'
    task :serve do
      server = ActiveRecord::ConnectionAdapters::Duckdb::QuackServer.new(
        database: ENV['DATABASE'] || ':memory:',
        bind: ENV.fetch('BIND', nil),
        token: ENV.fetch('QUACK_TOKEN', nil),
        extensions: ENV['QUACK_EXTENSIONS'].to_s.split(',').map(&:strip).reject(&:empty?),
        allow_other_hostname: %w[1 true yes].include?(ENV['QUACK_ALLOW_OTHER_HOSTNAME'].to_s.downcase)
      )

      server.start
      warn "DuckDB quack server: serving '#{server.database}' on '#{server.bind}' (Ctrl-C to stop)"
      warn 'WARNING: no token set; the server generated one at startup and all queries are allowed.' unless server.token

      server.wait
    ensure
      server&.stop
    end
  end
end
