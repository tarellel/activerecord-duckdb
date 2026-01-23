# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'fileutils'

# DuckLake Migration Tests
#
# These tests verify that Rails migrations can be used to create DuckLake tables
# with all supported column types, including DuckDB-specific types like unsigned integers.

RSpec.describe 'DuckLake Migrations' do
  # Helper to build DuckLake configuration with local storage
  def ducklake_config(temp_dir)
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

  describe 'creating tables with Rails migrations' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_migration_test') }
    let(:connection) { ActiveRecord::Base.connection }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    describe 'comprehensive column types table' do
      # This test creates a table with one example of each supported column type
      before do
        connection.create_table(:all_types, id: false) do |t|
          # Standard Rails types
          t.bigint :record_id, null: false
          t.datetime :recorded_at, null: false
          t.integer :count
          t.string :label
          t.boolean :active
          t.float :ratio
          t.decimal :amount, precision: 10, scale: 2
          t.decimal :coordinates, precision: 9, scale: 6

          # DuckDB signed integer types
          t.tinyint :tiny_val
          t.smallint :small_val

          # DuckDB unsigned integer types
          t.utinyint :unsigned_tiny
          t.usmallint :unsigned_small
          t.uinteger :unsigned_int
        end
      end

      it 'creates the table successfully' do
        expect(connection.table_exists?(:all_types)).to be true
      end

      it 'creates all columns' do
        columns = connection.columns(:all_types)
        column_names = columns.map(&:name)

        expected_columns = %w[
          record_id recorded_at count label active ratio amount coordinates
          tiny_val small_val unsigned_tiny unsigned_small unsigned_int
        ]

        expected_columns.each do |col_name|
          expect(column_names).to include(col_name), "Expected column '#{col_name}' to exist"
        end
      end

      describe 'column SQL types' do
        let(:columns) { connection.columns(:all_types) }
        let(:columns_by_name) { columns.index_by(&:name) }

        # Standard Rails types mapped to DuckDB
        {
          'record_id' => 'BIGINT',
          'recorded_at' => 'TIMESTAMP',
          'count' => 'INTEGER',
          'label' => 'VARCHAR',
          'active' => 'BOOLEAN',
          'ratio' => /REAL|FLOAT/i,
          'amount' => /DECIMAL\(10,\s*2\)/i,
          'coordinates' => /DECIMAL\(9,\s*6\)/i
        }.each do |column_name, expected_type|
          it "maps #{column_name} to correct SQL type" do
            col = columns_by_name[column_name]
            expect(col).not_to be_nil, "Column '#{column_name}' not found"
            if expected_type.is_a?(Regexp)
              expect(col.sql_type).to match(expected_type)
            else
              expect(col.sql_type.upcase).to eq(expected_type.upcase)
            end
          end
        end

        # DuckDB signed integer types
        it 'maps tiny_val to TINYINT' do
          col = columns_by_name['tiny_val']
          expect(col.sql_type.upcase).to eq('TINYINT')
        end

        it 'maps small_val to SMALLINT' do
          col = columns_by_name['small_val']
          expect(col.sql_type.upcase).to eq('SMALLINT')
        end

        # DuckDB unsigned integer types
        {
          'unsigned_tiny' => 'UTINYINT',
          'unsigned_small' => 'USMALLINT',
          'unsigned_int' => 'UINTEGER'
        }.each do |column_name, expected_type|
          it "maps #{column_name} to #{expected_type}" do
            col = columns_by_name[column_name]
            expect(col).not_to be_nil, "Column '#{column_name}' not found"
            expect(col.sql_type.upcase).to eq(expected_type)
          end
        end
      end

      describe 'NOT NULL constraints' do
        let(:columns) { connection.columns(:all_types) }
        let(:columns_by_name) { columns.index_by(&:name) }

        it 'enforces NOT NULL on required columns' do
          expect(columns_by_name['record_id'].null).to be false
          expect(columns_by_name['recorded_at'].null).to be false
        end

        it 'allows NULL on optional columns' do
          expect(columns_by_name['label'].null).to be true
          expect(columns_by_name['count'].null).to be true
        end
      end
    end

    describe 'DuckLake partitioning' do
      before do
        connection.create_table(:events, id: false) do |t|
          t.bigint :event_id, null: false
          t.datetime :occurred_at, null: false
          t.string :event_type
        end
      end

      it 'sets partitioning on a table' do
        connection.set_partitioned_by(
          :events,
          ['year(occurred_at)', 'month(occurred_at)', 'day(occurred_at)']
        )

        expect(connection.table_exists?(:events)).to be true
      end

      # Note: DuckLake does not support removing partitioning after it has been set.
      # Partitioning is a one-way operation. To change partitioning, you must recreate the table.

      it 'reflects partitioning in schema dumps' do
        connection.set_partitioned_by(
          :events,
          ['year(occurred_at)', 'month(occurred_at)']
        )

        require 'stringio'
        stream = StringIO.new
        ActiveRecord::SchemaDumper.ignore_tables = [/^ducklake_/]
        ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection_pool, stream)
        schema = stream.string

        # Schema should include the partition expressions
        expect(schema).to include('set_partitioned_by "events"')
        expect(schema).to include('year(occurred_at)')
        expect(schema).to include('month(occurred_at)')
      end
    end

    describe 'DuckLake options' do
      it 'sets parquet_version option' do
        expect { connection.set_ducklake_option('parquet_version', '2') }.not_to raise_error
      end

      it 'sets parquet_compression option' do
        expect { connection.set_ducklake_option('parquet_compression', 'zstd') }.not_to raise_error
      end
    end

    describe 'type_to_sql conversions' do
      # Test that type_to_sql correctly converts Rails types to DuckDB SQL types
      {
        bigint: 'BIGINT',
        integer: 'INTEGER',
        float: 'REAL',
        boolean: 'BOOLEAN',
        string: 'VARCHAR',
        datetime: 'TIMESTAMP',
        date: 'DATE',
        time: 'TIME',
        binary: 'BLOB',
        uuid: 'UUID',
        tinyint: 'TINYINT',
        smallint: 'SMALLINT',
        hugeint: 'HUGEINT',
        utinyint: 'UTINYINT',
        usmallint: 'USMALLINT',
        uinteger: 'UINTEGER',
        ubigint: 'UBIGINT',
        uhugeint: 'UHUGEINT',
        interval: 'INTERVAL'
      }.each do |rails_type, expected_sql|
        it "converts #{rails_type} to #{expected_sql}" do
          result = connection.type_to_sql(rails_type)
          expect(result).to eq(expected_sql)
        end
      end

      it 'converts decimal with precision and scale' do
        result = connection.type_to_sql(:decimal, precision: 9, scale: 6)
        expect(result).to eq('DECIMAL(9,6)')
      end

      it 'converts string with limit' do
        result = connection.type_to_sql(:string, limit: 100)
        expect(result).to eq('VARCHAR(100)')
      end

      it 'converts double to DOUBLE' do
        result = connection.type_to_sql(:double)
        expect(result).to eq('DOUBLE')
      end

      it 'converts real to REAL' do
        result = connection.type_to_sql(:real)
        expect(result).to eq('REAL')
      end
    end

    describe 'full migration workflow (create, alter, drop)' do
      it 'supports the full table lifecycle' do
        # Create
        connection.create_table(:workflow_test, id: false) do |t|
          t.bigint :id
          t.string :name
        end
        expect(connection.table_exists?(:workflow_test)).to be true

        # Add column
        connection.add_column(:workflow_test, :created_at, :datetime)
        columns = connection.columns(:workflow_test)
        expect(columns.map(&:name)).to include('created_at')

        # Drop
        connection.drop_table(:workflow_test)
        expect(connection.table_exists?(:workflow_test)).to be false
      end
    end
  end
end
