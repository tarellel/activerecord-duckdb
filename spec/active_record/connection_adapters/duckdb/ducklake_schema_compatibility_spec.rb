# frozen_string_literal: true

require 'spec_helper'
require 'stringio'
require 'tmpdir'
require 'fileutils'

# Tests for DuckLake-specific schema compatibility fixes
#
# These tests verify fixes for issues discovered when integrating DuckLake
# with Rails' schema management system, particularly around:
# - PRIMARY KEY constraints (not supported by DuckLake)
# - Schema dumping compatibility with other gems (e.g., Scenic)
# - Proper filtering of internal tables and metadata
RSpec.describe 'DuckLake Schema Compatibility' do
  let(:temp_dir) { Dir.mktmpdir('ducklake_compat_test') }

  def ducklake_config
    {
      adapter: 'duckdb',
      database: ':memory:',
      extensions: ['ducklake'],
      attachments: [{
        name: 'ducklake',
        connection_string: "ducklake:#{File.join(temp_dir, 'test.ducklake')}",
        options: "DATA_PATH '#{File.join(temp_dir, 'data')}'"
      }],
      use_database: 'ducklake'
    }
  end

  before do
    FileUtils.mkdir_p(File.join(temp_dir, 'data'))
    ActiveRecord::Base.establish_connection(ducklake_config)
    @connection = ActiveRecord::Base.connection
    ActiveRecord::SchemaDumper.ignore_tables = [/^ducklake_/]
  end

  after do
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
    FileUtils.rm_rf(temp_dir)
  end

  def dump_schema
    stream = StringIO.new
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection_pool, stream)
    stream.string
  end

  describe 'internal_string_options_for_primary_key' do
    # DuckLake doesn't support PRIMARY KEY constraints, so when Rails creates
    # internal tables like schema_migrations, we must omit the PRIMARY KEY
    it 'returns empty hash for DuckLake connections' do
      expect(@connection.internal_string_options_for_primary_key).to eq({})
    end

    it 'allows schema_migrations table creation without PRIMARY KEY' do
      # This should not raise "PRIMARY KEY/UNIQUE constraints are not supported in DuckLake"
      expect {
        @connection.create_table(:schema_migrations, id: false) do |t|
          t.string :version, **@connection.internal_string_options_for_primary_key
        end
      }.not_to raise_error

      expect(@connection.table_exists?(:schema_migrations)).to be true
      @connection.drop_table(:schema_migrations)
    end
  end

  describe 'views method' do
    it 'returns an array' do
      expect(@connection.views).to be_an(Array)
    end

    it 'only returns views from the main schema' do
      # DuckDB has many internal views in information_schema, pg_catalog, etc.
      # We should not return those
      views = @connection.views
      expect(views).not_to include('information_schema')
      expect(views.none? { |v| v.start_with?('pg_') }).to be true
    end
  end

  describe 'tables method with schema filtering' do
    it 'only returns tables from the main schema' do
      @connection.create_table(:user_table, id: false) do |t|
        t.string :name
      end

      tables = @connection.tables

      # Should include our table
      expect(tables).to include('user_table')

      # Should not include internal postgres scanner tables
      expect(tables.none? { |t| t.start_with?('pg_') }).to be true
      expect(tables.none? { |t| t.start_with?('_pg_') }).to be true

      # Should not include information_schema tables
      expect(tables).not_to include('columns')
      expect(tables).not_to include('tables')
      expect(tables).not_to include('schemata')

      @connection.drop_table(:user_table)
    end
  end

  describe 'schema dumper Scenic compatibility' do
    # When Scenic gem is present, it prepends modules to SchemaDumper that call
    # Scenic.database.views. Our DuckDB SchemaDumper must bypass this.

    it 'defined_views returns empty array' do
      dumper = ActiveRecord::ConnectionAdapters::Duckdb::SchemaDumper.create(@connection, {})
      expect(dumper.send(:defined_views)).to eq([])
    end

    it 'dumpable_views_in_database returns empty array' do
      dumper = ActiveRecord::ConnectionAdapters::Duckdb::SchemaDumper.create(@connection, {})
      expect(dumper.send(:dumpable_views_in_database)).to eq([])
    end
  end

  describe 'DuckLake options filtering' do
    # DuckLake stores various metadata options, but not all can be set via set_option.
    # Only parquet_version and parquet_compression are settable.

    before do
      @connection.set_ducklake_option('parquet_version', '2')
      @connection.set_ducklake_option('parquet_compression', 'zstd')
    end

    it 'includes settable options in schema dump' do
      schema = dump_schema
      expect(schema).to include('set_ducklake_option "parquet_compression"')
      expect(schema).to include('set_ducklake_option "parquet_version"')
    end
  end

  describe 'schema dump formatting' do
    before do
      @connection.create_table(:first_table, id: false) do |t|
        t.datetime :created_at
        t.string :name
      end
      @connection.set_partitioned_by(:first_table, ['month(created_at)'])

      @connection.create_table(:second_table, id: false) do |t|
        t.string :title
      end
    end

    after do
      @connection.drop_table(:second_table) if @connection.table_exists?(:second_table)
      @connection.drop_table(:first_table) if @connection.table_exists?(:first_table)
    end

    it 'has blank lines between tables but not after the last table' do
      schema = dump_schema

      # Blank line between tables: set_partitioned_by followed by blank line, then next create_table
      expect(schema).to match(/set_partitioned_by "first_table".*\n\n\s*create_table "second_table"/m)

      # No trailing blank line: last table's end followed directly by schema's end
      expect(schema).to match(/end\nend\n\z/)
    end

    it 'produces valid Ruby syntax' do
      schema = dump_schema
      expect { RubyVM::InstructionSequence.compile(schema) }.not_to raise_error
    end
  end
end

# Tests for regular DuckDB (non-DuckLake) to verify we don't break normal behavior
RSpec.describe 'DuckDB Schema Compatibility (non-DuckLake)' do
  before do
    ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
    @connection = ActiveRecord::Base.connection
  end

  after do
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
  end

  describe 'internal_string_options_for_primary_key' do
    it 'returns primary_key: true for non-DuckLake connections' do
      expect(@connection.internal_string_options_for_primary_key).to eq({ primary_key: true })
    end
  end

  describe 'tables method' do
    it 'only returns tables from the main schema' do
      @connection.create_table(:test_table, id: false) do |t|
        t.string :name
      end

      tables = @connection.tables

      expect(tables).to include('test_table')
      # Should not include internal tables
      expect(tables.none? { |t| t.start_with?('pg_') }).to be true

      @connection.drop_table(:test_table)
    end
  end
end
