# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::DuckdbAdapter do
  describe 'connection lifecycle' do
    it 'applies default settings when using connect!' do
      with_memory_connection do |conn|
        # Verify default settings were applied via configure_connection
        expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i).to eq(1)
      end
    end

    it 'applies custom settings when using connect!' do
      with_memory_connection(settings: { threads: 4 }) do |conn|
        expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i).to eq(4)
      end
    end

    it 'locks configuration after connect!' do
      with_memory_connection do |conn|
        expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'lock_configuration'", connection: conn)).to eq('true')
      end
    end

    it 'applies early settings before extensions could be loaded' do
      with_memory_connection do |conn|
        expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'allow_community_extensions'", connection: conn)).to eq('false')
      end
    end
  end

  describe 'connection via establish_connection (Rails integration)' do
    # Define a named test model for establish_connection tests
    class self::TestDuckdbModel < ActiveRecord::Base
      self.abstract_class = true
    end

    after do
      self.class::TestDuckdbModel.remove_connection if self.class::TestDuckdbModel.connected?
    end

    it 'applies default settings when using establish_connection' do
      self.class::TestDuckdbModel.establish_connection(
        adapter: 'duckdb',
        database: ':memory:'
      )

      conn = self.class::TestDuckdbModel.connection
      expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i).to eq(1)
    end

    it 'applies custom settings when using establish_connection' do
      self.class::TestDuckdbModel.establish_connection(
        adapter: 'duckdb',
        database: ':memory:',
        settings: { threads: 8 }
      )

      conn = self.class::TestDuckdbModel.connection
      expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i).to eq(8)
    end

    it 'locks configuration when using establish_connection' do
      self.class::TestDuckdbModel.establish_connection(
        adapter: 'duckdb',
        database: ':memory:'
      )

      conn = self.class::TestDuckdbModel.connection
      expect(query_value("SELECT value FROM duckdb_settings() WHERE name = 'lock_configuration'", connection: conn)).to eq('true')
    end
  end

  describe 'secrets configuration' do
    it 'creates an unnamed secret with type as key' do
      with_memory_connection(extensions: ['httpfs'], secrets: { s3: { key_id: 'test_key', secret: 'test_secret', region: 'us-east-1' } }) do |conn|
        result = conn.select_one('SELECT name, type, secret_string FROM duckdb_secrets()')
        expect(result['type']).to eq('s3')
        expect(result['name']).to eq('__default_s3')
        expect(result['secret_string']).to include('key_id=test_key')
        expect(result['secret_string']).to include('region=us-east-1')
      end
    end

    it 'creates a named secret with explicit type' do
      with_memory_connection(extensions: ['httpfs'], secrets: { my_s3_secret: { type: 's3', key_id: 'named_key', secret: 'named_secret' } }) do |conn|
        result = conn.select_one('SELECT name, type, secret_string FROM duckdb_secrets()')
        expect(result['name']).to eq('my_s3_secret')
        expect(result['type']).to eq('s3')
        expect(result['secret_string']).to include('key_id=named_key')
      end
    end

    it 'creates multiple secrets' do
      secrets = {
        s3: { key_id: 'default_key', secret: 'default_secret' },
        backup_creds: { type: 's3', key_id: 'backup_key', secret: 'backup_secret' }
      }

      with_memory_connection(extensions: ['httpfs'], secrets: secrets) do |conn|
        results = conn.select_all('SELECT name, type, secret_string FROM duckdb_secrets() ORDER BY name').to_a
        expect(results.size).to eq(2)

        # Unnamed secret has default name
        expect(results[0]['name']).to eq('__default_s3')
        expect(results[0]['secret_string']).to include('key_id=default_key')

        # Named secret
        expect(results[1]['name']).to eq('backup_creds')
        expect(results[1]['secret_string']).to include('key_id=backup_key')
      end
    end
  end

  describe 'configuration_locked?' do
    it 'returns true after initial configuration' do
      with_memory_connection do |conn|
        expect(conn.configuration_locked?).to be true
      end
    end

    it 'returns false when connection is not established' do
      adapter = described_class.allocate
      expect(adapter.configuration_locked?).to be false
    end
  end

  describe 'configure_connection skipping' do
    it 'skips reconfiguration when already locked' do
      with_memory_connection do |conn|
        # First configuration should have locked the connection
        expect(conn.configuration_locked?).to be true

        # Calling configure_connection again should not raise an error
        # because it skips when already configured
        expect { conn.configure_connection }.not_to raise_error
      end
    end
  end

  describe 'connection reset! (Rails API integration)' do
    it 'reset! does not raise when configuration is locked' do
      with_memory_connection do |conn|
        expect(conn.configuration_locked?).to be true

        # reset! calls configure_connection internally, which should skip
        # when already configured
        expect { conn.reset! }.not_to raise_error
        expect(conn.active?).to be true
      end
    end

    it 'connection remains functional after reset!' do
      with_memory_connection do |conn|
        conn.execute('CREATE TABLE reset_test (id INTEGER)')
        conn.execute('INSERT INTO reset_test VALUES (1)')

        conn.reset!

        # Connection should still work
        result = conn.execute('SELECT * FROM reset_test')
        expect(result.first.first).to eq(1)
      end
    end
  end

  describe 'connection reconnect! (Rails API integration)' do
    it 'reconnect! creates a fresh connection that can be configured' do
      with_memory_connection do |conn|
        expect(conn.configuration_locked?).to be true

        # reconnect! creates a new connection
        conn.reconnect!

        # New connection should be active and configured
        expect(conn.active?).to be true
        expect(conn.configuration_locked?).to be true
      end
    end

    it 'reconnect! allows fresh configuration on new connection' do
      with_memory_connection(settings: { threads: 2 }) do |conn|
        threads_before = query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i
        expect(threads_before).to eq(2)

        # After reconnect, the connection gets reconfigured with the same settings
        conn.reconnect!

        threads_after = query_value("SELECT value FROM duckdb_settings() WHERE name = 'threads'", connection: conn).to_i
        expect(threads_after).to eq(2)
      end
    end
  end

  describe 'connection configuration constants' do
    describe 'EARLY_SETTINGS' do
      it 'includes settings that must be applied before extensions' do
        expect(described_class::EARLY_SETTINGS).to include(*%i[allow_persistent_secrets allow_community_extensions])
      end
    end

    describe 'DEFAULT_SETTINGS' do
      it 'includes all expected default settings' do
        expect(described_class::DEFAULT_SETTINGS).to include(
          allow_persistent_secrets: false,
          allow_community_extensions: false,
          autoinstall_known_extensions: false,
          autoload_known_extensions: false,
          threads: 1,
          memory_limit: '1GiB',
          max_temp_directory_size: '4GiB'
        )
      end

      it 'does not include lock_configuration (handled separately at end)' do
        expect(described_class::DEFAULT_SETTINGS).not_to have_key(:lock_configuration)
      end
    end
  end
end
