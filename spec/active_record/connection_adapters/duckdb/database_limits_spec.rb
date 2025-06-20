# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::DatabaseLimits do
  let(:config) do
    {
      adapter: 'duckdb',
      database: ':memory:'
    }
  end

  let(:adapter) { ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(nil, nil, {}, config) }

  before { adapter.send(:connect) }

  after { adapter.disconnect }

  describe 'module inclusion' do
    it 'includes the DatabaseLimits module in DuckdbAdapter' do
      expect(adapter).to be_a(described_class)
    end

    it 'responds to logical limitation methods' do
      expect(adapter).to respond_to(:max_identifier_length)
      expect(adapter).to respond_to(:table_alias_length)
      expect(adapter).to respond_to(:table_name_length)
      expect(adapter).to respond_to(:index_name_length)
    end
  end

  describe '#max_identifier_length' do
    it 'returns 63 characters for PostgreSQL compatibility' do
      expect(adapter.max_identifier_length).to eq(63)
      expect(adapter.max_identifier_length).to be_a(Integer)
      expect(adapter.max_identifier_length).to be_positive
    end

    it 'is consistent across multiple calls' do
      first_call = adapter.max_identifier_length
      second_call = adapter.max_identifier_length
      expect(first_call).to eq(second_call)
    end
  end

  describe '#table_alias_length' do
    it 'delegates to max_identifier_length' do
      expect(adapter.table_alias_length).to eq(adapter.max_identifier_length)
      expect(adapter.table_alias_length).to eq(63)
    end
  end

  describe '#table_name_length' do
    it 'delegates to max_identifier_length' do
      expect(adapter.table_name_length).to eq(adapter.max_identifier_length)
      expect(adapter.table_name_length).to eq(63)
    end
  end

  describe '#index_name_length' do
    it 'delegates to max_identifier_length' do
      expect(adapter.index_name_length).to eq(adapter.max_identifier_length)
    end
  end

  describe 'private limit methods' do
    describe '#bind_params_length' do
      it 'returns 1000 bind parameters' do
        expect(adapter.send(:bind_params_length)).to eq(1000)
        expect(adapter.send(:bind_params_length)).to be_a(Integer)
        expect(adapter.send(:bind_params_length)).to be_positive
      end
    end

    describe '#insert_rows_length' do
      it 'returns 1000 rows for batch inserts' do
        expect(adapter.send(:insert_rows_length)).to eq(1000)
        expect(adapter.send(:insert_rows_length)).to be_a(Integer)
        expect(adapter.send(:insert_rows_length)).to be_positive
        expect(adapter.send(:insert_rows_length)).to eq(adapter.send(:bind_params_length))
      end
    end
  end

  describe 'PostgreSQL compatibility' do
    it 'uses PostgreSQL identifiers_length' do
      expect(adapter.max_identifier_length).to eq(63)
      expect(adapter.table_name_length).to eq(63)
      expect(adapter.index_name_length).to eq(63)
      expect(adapter.table_alias_length).to eq(63)
      expect(adapter.max_identifier_length).to be >= 30
    end
  end

  describe 'basic table operations' do
    let(:long_table_name) { 'a' * adapter.table_name_length }

    after do
      adapter.drop_table(long_table_name.to_sym, if_exists: true)
    end

    it 'supports table creation with names at the limit' do
      expect(long_table_name.length).to eq(63)
    end

    it 'creates tables with long names successfully' do
      expect do
        adapter.create_table(long_table_name.to_sym) do |t|
          t.string :name
        end
      end.not_to raise_error
    end
  end

  describe 'index operations' do
    let(:long_index_name) { 'i' * adapter.index_name_length }

    before do
      adapter.create_table(:limit_test) do |t|
        t.string :name
      end
    end

    after do
      adapter.drop_table(:limit_test, if_exists: true)
    end

    it 'supports index creation with names at the limit' do
      expect(long_index_name.length).to eq(63)
    end

    it 'creates indexes with long names successfully' do
      expect do
        adapter.execute("CREATE INDEX #{adapter.quote_column_name(long_index_name)} ON limit_test (name)")
      end.not_to raise_error
    end

    it 'verifies index was created' do
      adapter.execute("CREATE INDEX #{adapter.quote_column_name(long_index_name)} ON limit_test (name)")
      indexes = adapter.indexes('limit_test')
      created_index = indexes.find { |idx| idx.name == long_index_name }
      expect(created_index).not_to be_nil
    end
  end

  describe 'method visibility' do
    it 'exposes identifers as public' do
      expect(adapter.public_methods).to include(:max_identifier_length)
      expect(adapter.public_methods).to include(:table_alias_length)
      expect(adapter.public_methods).to include(:table_name_length)
      expect(adapter.public_methods).to include(:index_name_length)
    end

    it 'keeps identifers as private' do
      expect(adapter.respond_to?(:bind_params_length, :include_private)).to be true
      expect(adapter.respond_to?(:insert_rows_length, :include_private)).to be true
    end

    it 'does not expose identifiers publicly' do
      expect(adapter.public_methods).not_to include(:bind_params_length)
      expect(adapter.public_methods).not_to include(:insert_rows_length)
    end
  end

  describe 'limit consistency' do
    it 'maintains consistent table_name_length' do
      expect(adapter.table_name_length).to eq(adapter.max_identifier_length)
      expect(adapter.index_name_length).to eq(adapter.max_identifier_length)
      expect(adapter.table_alias_length).to eq(adapter.max_identifier_length)
    end

    it 'provides reasonable bind_params_length' do
      expect(adapter.send(:bind_params_length)).to be_between(100, 10_000)
      expect(adapter.send(:insert_rows_length)).to be_between(100, 10_000)
    end
  end

  describe 'value consistency across calls' do
    it 'returns same max_identifier_length' do
      first_call = adapter.max_identifier_length
      second_call = adapter.max_identifier_length
      expect(first_call).to eq(second_call)
    end

    it 'returns same bind_params_length' do
      first_call = adapter.send(:bind_params_length)
      second_call = adapter.send(:bind_params_length)
      expect(first_call).to eq(second_call)
    end

    it 'returns same insert_rows_length' do
      first_call = adapter.send(:insert_rows_length)
      second_call = adapter.send(:insert_rows_length)
      expect(first_call).to eq(second_call)
    end
  end

  describe 'Rails integration' do
    after do
      adapter.drop_table(:rails_integration_test, if_exists: true)
    end

    it 'supports Rails table creation syntax' do
      expect do
        adapter.create_table(:rails_integration_test) do |t|
          t.string :name
          t.string :email
          t.timestamps
        end
      end.not_to raise_error
    end

    it 'verifies table creation with basic query' do
      adapter.create_table(:rails_integration_test) do |t|
        t.string :name
        t.string :email
        t.timestamps
      end

      expect do
        adapter.execute('SELECT COUNT(*) FROM rails_integration_test')
      end.not_to raise_error
    end

    it 'supports Rails index creation' do
      adapter.create_table(:rails_integration_test) do |t|
        t.string :email
        t.string :username
      end

      expect do
        adapter.add_index(:rails_integration_test, :email, unique: true, name: 'idx_email_unique')
      end.not_to raise_error
    end

    it 'supports composite index creation' do
      adapter.create_table(:rails_integration_test) do |t|
        t.string :email
        t.string :username
      end

      expect do
        adapter.add_index(:rails_integration_test, %i[email username], name: 'idx_email_username')
      end.not_to raise_error
    end

    it 'respects index name length limits' do
      adapter.create_table(:rails_integration_test) do |t|
        t.string :email
        t.string :username
      end

      adapter.add_index(:rails_integration_test, :email, unique: true, name: 'idx_email_unique')
      adapter.add_index(:rails_integration_test, %i[email username], name: 'idx_email_username')

      indexes = adapter.indexes(:rails_integration_test)
      indexes.each do |index|
        expect(index.name.length).to be <= adapter.index_name_length
      end
    end
  end
end
