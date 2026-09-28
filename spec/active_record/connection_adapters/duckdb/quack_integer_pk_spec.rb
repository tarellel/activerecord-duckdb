# frozen_string_literal: true

require 'spec_helper'

# Unit specs for the integer-primary-key support over a quack connection.
#
# The full behavior (migrate + CRUD against a live quack server with integer PKs)
# requires a running remote DuckDB server, which the unit suite avoids. These specs
# exercise the individual decisions the adapter makes in quack mode by allocating a
# bare adapter, flipping @quack_url, and mocking the connection boundary. The
# end-to-end path is verified manually against DuckDB >= 1.5.3.
RSpec.describe 'DuckDB quack integer primary key support' do
  # An allocated adapter with quack mode on/off and a minimal config.
  def build_adapter(quack: true)
    adapter = ActiveRecord::ConnectionAdapters::DuckdbAdapter.allocate
    adapter.instance_variable_set(:@quack_url, quack ? 'quack:localhost:9494' : nil)
    adapter.instance_variable_set(:@config, {})
    adapter
  end

  describe '#quack_enabled?' do
    it 'is true when a quack url is set' do
      expect(build_adapter(quack: true).quack_enabled?).to be true
    end

    it 'is false otherwise' do
      expect(build_adapter(quack: false).quack_enabled?).to be false
    end
  end

  describe '#prefetch_primary_key?' do
    it 'prefetches in quack mode (no DB default is possible over quack)' do
      expect(build_adapter(quack: true).prefetch_primary_key?('users')).to be true
    end

    it 'does not prefetch normally (DuckDB fills the id from a sequence default)' do
      expect(build_adapter(quack: false).prefetch_primary_key?('users')).to be false
    end
  end

  describe 'INSERT ... RETURNING support' do
    it 'is disabled over quack (RETURNING is unreliable there)' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:ducklake?).and_return(false)
      expect(adapter.supports_insert_returning?).to be false
      expect(adapter.use_insert_returning?).to be false
    end

    it 'is enabled for a regular DuckDB connection' do
      adapter = build_adapter(quack: false)
      allow(adapter).to receive(:ducklake?).and_return(false)
      expect(adapter.supports_insert_returning?).to be true
    end
  end

  describe '#next_sequence_value' do
    it 'returns the actual next integer from the server in quack mode' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:quack_query_value).with("SELECT nextval('users_id_seq')").and_return(7)
      expect(adapter.next_sequence_value('users_id_seq')).to eq(7)
    end

    it 'returns a nextval() SQL expression outside quack mode' do
      expect(build_adapter(quack: false).next_sequence_value('users_id_seq'))
        .to eq("nextval('users_id_seq')")
    end
  end

  describe '#quack_inline_binds' do
    it 'replaces ? placeholders in order with quoted bind values' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:type_casted_binds).and_return(['bob', 5])
      expect(adapter.quack_inline_binds('UPDATE t SET name = ? WHERE id = ?', :binds))
        .to eq("UPDATE t SET name = 'bob' WHERE id = 5")
    end

    it 'escapes single quotes in string binds' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:type_casted_binds).and_return(["O'Brien"])
      expect(adapter.quack_inline_binds('UPDATE t SET name = ?', :binds))
        .to eq("UPDATE t SET name = 'O''Brien'")
    end

    it 'returns the SQL unchanged when there are no binds' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:type_casted_binds).and_return([])
      expect(adapter.quack_inline_binds('DELETE FROM t WHERE id = 1', [])).to eq('DELETE FROM t WHERE id = 1')
    end
  end

  describe '#exec_update / #exec_delete in quack mode' do
    it 'routes UPDATE through quack_query and returns the affected-row count' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:type_casted_binds).and_return([])
      allow(adapter).to receive(:log).and_yield
      allow(adapter).to receive(:quack_query_value).with("UPDATE users SET name = 'x'").and_return(4)
      expect(adapter.exec_update("UPDATE users SET name = 'x'")).to eq(4)
    end

    it 'routes DELETE through quack_query and returns the affected-row count' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:type_casted_binds).and_return([])
      allow(adapter).to receive(:log).and_yield
      allow(adapter).to receive(:quack_query_value).with('DELETE FROM users WHERE id = 2').and_return(1)
      expect(adapter.exec_delete('DELETE FROM users WHERE id = 2')).to eq(1)
    end
  end

  describe '#insert in quack mode' do
    it 'runs the insert and hands back the prefetched id (array when returning requested)' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:to_sql_and_binds).and_return(['INSERT INTO users ...', []])
      allow(adapter).to receive(:exec_insert)
      expect(adapter.insert(:arel, 'Create', 'id', 42, nil, [], returning: ['id'])).to eq([42])
    end

    it 'hands back the prefetched id as a scalar when no returning columns are requested' do
      adapter = build_adapter(quack: true)
      allow(adapter).to receive(:to_sql_and_binds).and_return(['INSERT INTO users ...', []])
      allow(adapter).to receive(:exec_insert)
      expect(adapter.insert(:arel, 'Create', 'id', 42, nil, [], returning: nil)).to eq(42)
    end
  end
end
