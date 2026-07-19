# frozen_string_literal: true

require 'spec_helper'

# Specs for the optional quack server launcher.
#
# A quack server needs a long-lived process and a real network listener, which
# isn't appropriate for the unit suite, so these specs assert on the SQL the
# server emits and on how it opens the database (mocking DuckDB), rather than
# actually binding a port. The emitted CALL quack_serve(...) should be verified
# end-to-end against DuckDB >= 1.5.3 when a server environment is available.
RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::QuackServer do
  describe '#serve_sql' do
    it 'uses the default bind and no params when nothing is configured' do
      expect(described_class.new.serve_sql).to eq("CALL quack_serve('quack:localhost:9494')")
    end

    it 'honors a custom bind URI' do
      server = described_class.new(bind: 'quack:0.0.0.0:9494')
      expect(server.serve_sql).to eq("CALL quack_serve('quack:0.0.0.0:9494')")
    end

    it 'includes the token as a named parameter when given' do
      server = described_class.new(token: 'super_secret')
      expect(server.serve_sql).to eq("CALL quack_serve('quack:localhost:9494', token => 'super_secret')")
    end

    it 'includes allow_other_hostname when enabled' do
      server = described_class.new(bind: 'quack:0.0.0.0:9494', allow_other_hostname: true)
      expect(server.serve_sql).to eq("CALL quack_serve('quack:0.0.0.0:9494', allow_other_hostname => true)")
    end

    it 'combines token and allow_other_hostname in order' do
      server = described_class.new(bind: 'quack:0.0.0.0:9494', token: 'abcd', allow_other_hostname: true)
      expect(server.serve_sql).to eq(
        "CALL quack_serve('quack:0.0.0.0:9494', token => 'abcd', allow_other_hostname => true)"
      )
    end

    it 'escapes single quotes in the token' do
      server = described_class.new(token: "a'bc")
      expect(server.serve_sql).to eq("CALL quack_serve('quack:localhost:9494', token => 'a''bc')")
    end
  end

  describe '#startup_sql' do
    it 'installs and loads quack, then serves' do
      expect(described_class.new.startup_sql).to eq(
        ['INSTALL quack', 'LOAD quack', "CALL quack_serve('quack:localhost:9494')"]
      )
    end

    it 'installs and loads any extra extensions before serving' do
      server = described_class.new(extensions: %w[httpfs postgres_scanner])
      expect(server.startup_sql).to eq(
        [
          'INSTALL quack', 'LOAD quack',
          'INSTALL httpfs', 'LOAD httpfs',
          'INSTALL postgres_scanner', 'LOAD postgres_scanner',
          "CALL quack_serve('quack:localhost:9494')"
        ]
      )
    end
  end

  describe 'defaults and normalization' do
    it 'defaults to an in-memory database' do
      expect(described_class.new.database).to eq(':memory:')
    end

    it 'falls back to the default bind when bind is blank' do
      expect(described_class.new(bind: '').bind).to eq('quack:localhost:9494')
    end

    it 'treats a blank token as no token' do
      expect(described_class.new(token: '').token).to be_nil
    end

    it 'raises when a non-blank token is shorter than the minimum length' do
      expect { described_class.new(token: 'abc') }
        .to raise_error(ArgumentError, /at least 4 characters/)
    end

    it 'accepts a token at the minimum length' do
      expect(described_class.new(token: 'abcd').token).to eq('abcd')
    end
  end

  describe '#start' do
    let(:raw) { instance_double(DuckDB::Connection, execute: nil, close: nil) }
    let(:db) { instance_double(DuckDB::Database, connect: raw) }

    it 'opens an in-memory database with no path' do
      allow(DuckDB::Database).to receive(:open).with(no_args).and_return(db)
      described_class.new(database: ':memory:').start
      expect(DuckDB::Database).to have_received(:open).with(no_args)
    end

    it 'opens a file database with the given path' do
      allow(DuckDB::Database).to receive(:open).with('db/shared.duckdb').and_return(db)
      described_class.new(database: 'db/shared.duckdb').start
      expect(DuckDB::Database).to have_received(:open).with('db/shared.duckdb')
    end

    it 'executes each startup statement in order on the connection' do
      allow(DuckDB::Database).to receive(:open).and_return(db)
      server = described_class.new(token: 'super_secret')
      server.start

      expect(raw).to have_received(:execute).with('INSTALL quack').ordered
      expect(raw).to have_received(:execute).with('LOAD quack').ordered
      expect(raw).to have_received(:execute).with(
        "CALL quack_serve('quack:localhost:9494', token => 'super_secret')"
      ).ordered
    end

    it 'exposes the serving connection' do
      allow(DuckDB::Database).to receive(:open).and_return(db)
      server = described_class.new.start
      expect(server.connection).to eq(raw)
    end
  end

  describe '#stop' do
    it 'closes the connection and clears it' do
      raw = instance_double(DuckDB::Connection, execute: nil, close: nil)
      db = instance_double(DuckDB::Database, connect: raw)
      allow(DuckDB::Database).to receive(:open).and_return(db)

      server = described_class.new.start
      server.stop

      expect(raw).to have_received(:close)
      expect(server.connection).to be_nil
    end
  end
end
