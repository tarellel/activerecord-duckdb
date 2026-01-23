# frozen_string_literal: true

require 'spec_helper'
require 'stringio'

# Tests for DuckDB Schema Dumper
#
# These tests verify that ActiveRecord::SchemaDumper correctly generates
# schema.rb content for DuckDB tables and columns.
RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::SchemaDumper do
  before do
    ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
    @connection = ActiveRecord::Base.connection
  end

  after do
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
  end

  def dump_schema
    stream = StringIO.new
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection_pool, stream)
    stream.string
  end

  describe 'standard column types' do
    before do
      @connection.create_table(:standard_types, id: false) do |t|
        t.bigint :bigint_col
        t.integer :integer_col
        t.string :string_col
        t.string :string_with_limit, limit: 100
        t.text :text_col
        t.boolean :boolean_col
        t.float :float_col
        t.decimal :decimal_col, precision: 10, scale: 2
        t.date :date_col
        t.time :time_col
        t.datetime :datetime_col
        t.binary :binary_col
        t.uuid :uuid_col
      end
    end

    after do
      @connection.drop_table(:standard_types) if @connection.table_exists?(:standard_types)
    end

    it 'dumps bigint columns' do
      expect(dump_schema).to include('t.bigint "bigint_col"')
    end

    it 'dumps integer columns' do
      expect(dump_schema).to include('t.integer "integer_col"')
    end

    it 'dumps string columns' do
      expect(dump_schema).to include('t.string "string_col"')
    end

    it 'dumps string columns with limit' do
      schema = dump_schema
      expect(schema).to include('string_with_limit')
      # Limit should be preserved if different from default
    end

    it 'dumps text columns as string (DuckDB uses VARCHAR for text)' do
      # DuckDB doesn't have a native TEXT type - it uses VARCHAR
      expect(dump_schema).to include('"text_col"')
    end

    it 'dumps boolean columns' do
      expect(dump_schema).to include('t.boolean "boolean_col"')
    end

    it 'dumps float columns' do
      expect(dump_schema).to include('t.float "float_col"')
    end

    it 'dumps decimal columns with precision and scale' do
      schema = dump_schema
      expect(schema).to include('decimal_col')
      expect(schema).to include('precision: 10')
      expect(schema).to include('scale: 2')
    end

    it 'dumps date columns' do
      expect(dump_schema).to include('t.date "date_col"')
    end

    it 'dumps time columns' do
      expect(dump_schema).to include('t.time "time_col"')
    end

    it 'dumps datetime columns' do
      expect(dump_schema).to include('t.datetime "datetime_col"')
    end

    it 'dumps binary columns' do
      expect(dump_schema).to include('t.binary "binary_col"')
    end

    it 'dumps uuid columns' do
      expect(dump_schema).to include('t.uuid "uuid_col"')
    end
  end

  describe 'DuckDB-specific signed integer types' do
    before do
      @connection.create_table(:signed_int_types, id: false) do |t|
        t.tinyint :tinyint_col
        t.smallint :smallint_col
        t.hugeint :hugeint_col
      end
    end

    after do
      @connection.drop_table(:signed_int_types) if @connection.table_exists?(:signed_int_types)
    end

    it 'dumps tinyint columns' do
      expect(dump_schema).to include('t.tinyint "tinyint_col"')
    end

    it 'dumps smallint columns' do
      expect(dump_schema).to include('t.smallint "smallint_col"')
    end

    it 'dumps hugeint columns' do
      expect(dump_schema).to include('t.hugeint "hugeint_col"')
    end
  end

  describe 'DuckDB-specific unsigned integer types' do
    before do
      @connection.create_table(:unsigned_int_types, id: false) do |t|
        t.utinyint :utinyint_col
        t.usmallint :usmallint_col
        t.uinteger :uinteger_col
        t.ubigint :ubigint_col
        t.uhugeint :uhugeint_col
      end
    end

    after do
      @connection.drop_table(:unsigned_int_types) if @connection.table_exists?(:unsigned_int_types)
    end

    it 'dumps utinyint columns' do
      expect(dump_schema).to include('t.utinyint "utinyint_col"')
    end

    it 'dumps usmallint columns' do
      expect(dump_schema).to include('t.usmallint "usmallint_col"')
    end

    it 'dumps uinteger columns' do
      expect(dump_schema).to include('t.uinteger "uinteger_col"')
    end

    it 'dumps ubigint columns' do
      expect(dump_schema).to include('t.ubigint "ubigint_col"')
    end

    it 'dumps uhugeint columns' do
      expect(dump_schema).to include('t.uhugeint "uhugeint_col"')
    end
  end

  describe 'interval type' do
    before do
      @connection.create_table(:interval_types, id: false) do |t|
        t.interval :interval_col
      end
    end

    after do
      @connection.drop_table(:interval_types) if @connection.table_exists?(:interval_types)
    end

    it 'dumps interval columns' do
      expect(dump_schema).to include('t.interval "interval_col"')
    end
  end

  describe 'column constraints' do
    before do
      @connection.create_table(:constrained_cols, id: false) do |t|
        t.string :required_col, null: false
        t.string :optional_col, null: true
        t.integer :default_col, default: 42
        t.boolean :bool_default, default: true
      end
    end

    after do
      @connection.drop_table(:constrained_cols) if @connection.table_exists?(:constrained_cols)
    end

    it 'dumps null: false constraint' do
      expect(dump_schema).to include('null: false')
    end

    it 'does not include null: true (default)' do
      schema = dump_schema
      expect(schema).not_to include('null: true')
    end

    it 'dumps default values' do
      schema = dump_schema
      expect(schema).to include('default_col')
      expect(schema).to match(/default:/)
    end
  end

  describe 'primary key handling' do
    it 'dumps tables with default primary key' do
      @connection.create_table(:pk_default) do |t|
        t.string :name
      end

      schema = dump_schema
      expect(schema).to include('create_table "pk_default"')
      expect(schema).not_to include('id: false')

      @connection.drop_table(:pk_default)
    end

    it 'dumps tables without primary key' do
      @connection.create_table(:pk_none, id: false) do |t|
        t.string :name
      end

      schema = dump_schema
      expect(schema).to include('create_table "pk_none", id: false')

      @connection.drop_table(:pk_none)
    end
  end

  describe 'TIME vs TIMESTAMP disambiguation' do
    before do
      @connection.create_table(:time_types, id: false) do |t|
        t.time :time_only
        t.datetime :timestamp_col
      end
    end

    after do
      @connection.drop_table(:time_types) if @connection.table_exists?(:time_types)
    end

    it 'correctly distinguishes TIME from TIMESTAMP' do
      schema = dump_schema
      expect(schema).to include('t.time "time_only"')
      expect(schema).to include('t.datetime "timestamp_col"')
    end
  end

  describe 'schema roundtrip' do
    before do
      @connection.create_table(:roundtrip_test, id: false) do |t|
        t.bigint :bigint_col
        t.integer :integer_col
        t.string :string_col
        t.decimal :decimal_col, precision: 10, scale: 2
        t.boolean :boolean_col
        t.datetime :datetime_col
        t.tinyint :tinyint_col
        t.utinyint :utinyint_col
        t.interval :interval_col
        t.uuid :uuid_col
      end
    end

    after do
      @connection.drop_table(:roundtrip_test) if @connection.table_exists?(:roundtrip_test)
    end

    it 'dumps schema that preserves all DuckDB types' do
      schema = dump_schema

      # All types should be preserved
      expect(schema).to include('t.bigint "bigint_col"')
      expect(schema).to include('t.integer "integer_col"')
      expect(schema).to include('t.string "string_col"')
      expect(schema).to include('decimal_col')
      expect(schema).to include('t.boolean "boolean_col"')
      expect(schema).to include('t.datetime "datetime_col"')
      expect(schema).to include('t.tinyint "tinyint_col"')
      expect(schema).to include('t.utinyint "utinyint_col"')
      expect(schema).to include('t.interval "interval_col"')
      expect(schema).to include('t.uuid "uuid_col"')
    end

    it 'produces schema that can be parsed as valid Ruby' do
      schema = dump_schema
      # Extract just the create_table block
      create_block = schema[/create_table "roundtrip_test".*?end/m]
      expect(create_block).not_to be_nil

      # Verify it's valid Ruby (won't raise SyntaxError)
      expect { RubyVM::InstructionSequence.compile(create_block) }.not_to raise_error
    end
  end

  describe 'multiple tables' do
    before do
      @connection.create_table(:table_one, id: false) do |t|
        t.string :name
      end
      @connection.create_table(:table_two, id: false) do |t|
        t.integer :count
      end
    end

    after do
      @connection.drop_table(:table_one) if @connection.table_exists?(:table_one)
      @connection.drop_table(:table_two) if @connection.table_exists?(:table_two)
    end

    it 'dumps all tables' do
      schema = dump_schema
      expect(schema).to include('create_table "table_one"')
      expect(schema).to include('create_table "table_two"')
    end
  end

  describe 'edge cases' do
    it 'handles tables with single column' do
      @connection.create_table(:single_col_table, id: false) do |t|
        t.integer :only_col
      end

      expect { dump_schema }.not_to raise_error
      expect(dump_schema).to include('single_col_table')

      @connection.drop_table(:single_col_table)
    end

    it 'handles tables with many columns' do
      @connection.create_table(:many_cols, id: false) do |t|
        20.times { |i| t.string "col_#{i}" }
      end

      schema = dump_schema
      20.times { |i| expect(schema).to include("col_#{i}") }

      @connection.drop_table(:many_cols)
    end
  end
end

# DuckLake-specific schema dumping tests
# These require a DuckLake connection with partitioning support
RSpec.describe 'DuckLake Schema Dumping' do
  let(:temp_dir) { Dir.mktmpdir('ducklake_schema_test') }

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
    require 'tmpdir'
    require 'fileutils'
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

  describe 'partitioned tables' do
    before do
      @connection.create_table(:events, id: false) do |t|
        t.bigint :id
        t.datetime :created_at
        t.string :event_type
      end
    end

    after do
      @connection.drop_table(:events) if @connection.table_exists?(:events)
    end

    it 'dumps tables with single partition expression' do
      @connection.set_partitioned_by(:events, ['month(created_at)'])

      schema = dump_schema
      expect(schema).to include('create_table "events"')
      expect(schema).to include('set_partitioned_by "events"')
      expect(schema).to include('month(created_at)')
    end

    it 'dumps tables with multiple partition expressions' do
      @connection.set_partitioned_by(:events, ['year(created_at)', 'month(created_at)'])

      schema = dump_schema
      expect(schema).to include('set_partitioned_by "events"')
      expect(schema).to include('year(created_at)')
      expect(schema).to include('month(created_at)')
    end

    it 'preserves partition expression order' do
      @connection.set_partitioned_by(:events, ['year(created_at)', 'month(created_at)', 'day(created_at)'])

      schema = dump_schema
      # The expressions should appear in order
      year_pos = schema.index('year(created_at)')
      month_pos = schema.index('month(created_at)')
      day_pos = schema.index('day(created_at)')

      expect(year_pos).to be < month_pos
      expect(month_pos).to be < day_pos
    end
  end

  describe 'non-partitioned tables' do
    it 'does not include set_partitioned_by for regular tables' do
      @connection.create_table(:simple_table, id: false) do |t|
        t.string :name
      end

      schema = dump_schema
      expect(schema).to include('create_table "simple_table"')
      expect(schema).not_to include('set_partitioned_by "simple_table"')

      @connection.drop_table(:simple_table)
    end
  end

  describe 'mixed tables' do
    it 'correctly handles both partitioned and non-partitioned tables' do
      @connection.create_table(:partitioned_logs, id: false) do |t|
        t.datetime :logged_at
        t.string :message
      end
      @connection.set_partitioned_by(:partitioned_logs, ['day(logged_at)'])

      @connection.create_table(:users, id: false) do |t|
        t.string :name
        t.string :email
      end

      schema = dump_schema

      # Partitioned table should have set_partitioned_by
      expect(schema).to include('set_partitioned_by "partitioned_logs"')
      expect(schema).to include('day(logged_at)')

      # Non-partitioned table should not
      expect(schema).not_to include('set_partitioned_by "users"')

      @connection.drop_table(:partitioned_logs)
      @connection.drop_table(:users)
    end
  end

  describe 'DuckDB types in DuckLake' do
    it 'dumps all DuckDB-specific types correctly in DuckLake mode' do
      @connection.create_table(:typed_table, id: false) do |t|
        t.bigint :bigint_col
        t.tinyint :tinyint_col
        t.smallint :smallint_col
        t.utinyint :utinyint_col
        t.usmallint :usmallint_col
        t.uinteger :uinteger_col
        t.ubigint :ubigint_col
        t.interval :interval_col
        t.uuid :uuid_col
        t.decimal :decimal_col, precision: 10, scale: 2
      end

      schema = dump_schema

      expect(schema).to include('t.bigint "bigint_col"')
      expect(schema).to include('t.tinyint "tinyint_col"')
      expect(schema).to include('t.smallint "smallint_col"')
      expect(schema).to include('t.utinyint "utinyint_col"')
      expect(schema).to include('t.usmallint "usmallint_col"')
      expect(schema).to include('t.uinteger "uinteger_col"')
      expect(schema).to include('t.ubigint "ubigint_col"')
      expect(schema).to include('t.interval "interval_col"')
      expect(schema).to include('t.uuid "uuid_col"')
      expect(schema).to include('decimal_col')

      @connection.drop_table(:typed_table)
    end
  end

  describe 'DuckLake options' do
    it 'dumps parquet_version option' do
      @connection.set_ducklake_option('parquet_version', '2')

      schema = dump_schema
      expect(schema).to include('set_ducklake_option')
      expect(schema).to include('parquet_version')
    end

    it 'dumps parquet_compression option' do
      @connection.set_ducklake_option('parquet_compression', 'zstd')

      schema = dump_schema
      expect(schema).to include('set_ducklake_option')
      expect(schema).to include('parquet_compression')
      expect(schema).to include('zstd')
    end

    it 'dumps multiple options in alphabetical order' do
      @connection.set_ducklake_option('parquet_version', '2')
      @connection.set_ducklake_option('parquet_compression', 'snappy')

      schema = dump_schema
      # Both options should be present
      expect(schema).to include('parquet_compression')
      expect(schema).to include('parquet_version')

      # They should appear in alphabetical order (compression before version)
      compression_pos = schema.index('parquet_compression')
      version_pos = schema.index('parquet_version')
      expect(compression_pos).to be < version_pos
    end

    it 'places options before table definitions' do
      @connection.set_ducklake_option('parquet_compression', 'gzip')

      @connection.create_table(:some_table, id: false) do |t|
        t.string :name
      end

      schema = dump_schema
      options_pos = schema.index('set_ducklake_option')
      table_pos = schema.index('create_table "some_table"')

      expect(options_pos).to be < table_pos

      @connection.drop_table(:some_table)
    end

  end

  describe 'table-level options' do
    before do
      @connection.create_table(:events, id: false) do |t|
        t.bigint :id
        t.datetime :created_at
      end
    end

    after do
      @connection.drop_table(:events) if @connection.table_exists?(:events)
    end

    it 'dumps table-specific parquet_compression' do
      @connection.set_ducklake_option('parquet_compression', 'zstd', :events)

      schema = dump_schema
      expect(schema).to include('set_ducklake_option "parquet_compression", "zstd", "events"')
    end

    it 'places table options after the table definition' do
      @connection.set_ducklake_option('parquet_compression', 'gzip', :events)

      schema = dump_schema
      table_pos = schema.index('create_table "events"')
      option_pos = schema.index('"parquet_compression", "gzip", "events"')

      expect(option_pos).to be > table_pos
    end

    it 'handles multiple tables with different options' do
      @connection.set_ducklake_option('parquet_compression', 'zstd', :events)

      @connection.create_table(:logs, id: false) do |t|
        t.string :message
      end
      @connection.set_ducklake_option('parquet_compression', 'snappy', :logs)

      schema = dump_schema
      expect(schema).to include('"parquet_compression", "zstd", "events"')
      expect(schema).to include('"parquet_compression", "snappy", "logs"')

      @connection.drop_table(:logs)
    end

    it 'does not mix up global and table options' do
      @connection.set_ducklake_option('parquet_version', '2')  # global
      @connection.set_ducklake_option('parquet_compression', 'zstd', :events)  # table

      schema = dump_schema

      # Global option should not have table name
      global_match = schema.match(/set_ducklake_option "parquet_version", "[^"]+"\s*$/)
      expect(global_match).not_to be_nil

      # Table option should have table name
      expect(schema).to include('"parquet_compression", "zstd", "events"')
    end
  end
end
