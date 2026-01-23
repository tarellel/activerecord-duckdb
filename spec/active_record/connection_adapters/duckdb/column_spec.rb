# frozen_string_literal: true

require 'spec_helper'

# Load version-specific column builder
if ActiveRecord::VERSION::MAJOR > 8 ||
   (ActiveRecord::VERSION::MAJOR == 8 && ActiveRecord::VERSION::MINOR >= 1)
  require_relative '../../../support/column_builder_rails81'
else
  require_relative '../../../support/column_builder_rails80'
end

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::Column do
  # Include version-specific column builder
  if ActiveRecord::VERSION::MAJOR > 8 ||
     (ActiveRecord::VERSION::MAJOR == 8 && ActiveRecord::VERSION::MINOR >= 1)
    include ColumnBuilderRails81
  else
    include ColumnBuilderRails80
  end

  # Helper method to create fresh metadata for each test
  def fresh_metadata(sql_type: 'INTEGER', type: :integer)
    ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
      sql_type:,
      type:
    )
  end

  describe 'initialization' do
    it 'creates a column with basic attributes' do
      metadata = fresh_metadata
      column = build_column('test_column', 'default_value', metadata, true)

      expect(column.name).to eq('test_column')
      expect(column.default).to eq('default_value')
      expect(column.sql_type_metadata).to eq(metadata)
      expect(column.null).to be true
    end

    it 'accepts DuckDB-specific options' do
      metadata = fresh_metadata
      column = build_column(
        'test_column',
        nil,
        metadata,
        false,
        auto_increment: true,
        rowid: true,
        generated_type: :stored,
        extra: 'GENERATED ALWAYS AS (id + 1) STORED'
      )

      expect(column.auto_increment?).to be true
      expect(column.rowid).to be true
      expect(column.extra).to eq('GENERATED ALWAYS AS (id + 1) STORED')
    end

    it 'handles nil options gracefully' do
      metadata = fresh_metadata
      column = build_column('test_column', nil, metadata, true)

      expect(column.auto_increment?).to be false
      expect(column.rowid).to be_nil
      expect(column.extra).to be_nil
    end
  end

  describe '#has_default?' do
    it 'returns true for non-virtual columns with default' do
      metadata = fresh_metadata
      column = build_column('test', 'default_value', metadata, true)
      expect(column.has_default?).to be true
    end

    it 'returns false for non-virtual columns without default' do
      metadata = fresh_metadata
      column = build_column('test', nil, metadata, true)
      expect(column.has_default?).to be false
    end
  end

  describe '#auto_increment?' do
    it 'returns true when auto_increment is true' do
      metadata = fresh_metadata
      column = build_column('id', nil, metadata, false, auto_increment: true)
      expect(column.auto_increment?).to be true
    end
  end

  describe '#auto_incremented_by_db?' do
    it 'returns true when auto_increment is true' do
      metadata = fresh_metadata
      column = build_column('id', nil, metadata, false, auto_increment: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns true when rowid is true' do
      metadata = fresh_metadata
      column = build_column('rowid', nil, metadata, false, rowid: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns true when both are true' do
      metadata = fresh_metadata
      column = build_column('id', nil, metadata, false, auto_increment: true, rowid: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns false when neither is true' do
      metadata = fresh_metadata
      column = build_column('name', nil, metadata, false)
      expect(column.auto_incremented_by_db?).to be false
    end
  end

  describe '#rowid' do
    it 'exposes rowid attribute when set' do
      metadata = fresh_metadata
      column = build_column('test_rowid_true', nil, metadata, true, rowid: true)
      expect(column.rowid).to be true
    end

    it 'defaults to nil when not specified' do
      metadata = fresh_metadata
      column = build_column('test_rowid_nil', nil, metadata, true)
      expect(column.rowid).to be_nil
    end
  end

  describe 'inheritance from ConnectionAdapters::Column' do
    let(:column) do
      metadata = fresh_metadata
      build_column('test', 'default', metadata, true)
    end

    it 'inherits from ConnectionAdapters::Column' do
      expect(described_class).to be < ActiveRecord::ConnectionAdapters::Column
    end

    it 'delegates type to sql_type_metadata' do
      expect(column.type).to eq(:integer)
    end

    it 'responds to parent class methods' do
      expect(column).to respond_to(:name)
      expect(column).to respond_to(:default)
      expect(column).to respond_to(:null)
      expect(column).to respond_to(:sql_type_metadata)
    end
  end

  describe 'real-world scenarios' do
    it 'handles sequence-based primary key columns' do
      metadata = fresh_metadata(sql_type: 'BIGINT', type: :bigint)
      column = build_column('id', nil, metadata, false, auto_increment: true)

      expect(column.name).to eq('id')
      expect(column.type).to eq(:bigint)
      expect(column.null).to be false
      expect(column.auto_increment?).to be true
      expect(column.auto_incremented_by_db?).to be true
      expect(column.virtual?).to be false
    end

    it 'handles UUID primary key columns' do
      metadata = fresh_metadata(sql_type: 'UUID', type: :uuid)
      column = build_column('id', nil, metadata, false)

      expect(column.name).to eq('id')
      expect(column.type).to eq(:uuid)
      expect(column.null).to be false
      expect(column.auto_increment?).to be false
      expect(column.auto_incremented_by_db?).to be false
    end

    it 'handles basic columns without special features' do
      metadata = fresh_metadata
      column = build_column('total', nil, metadata, true)

      expect(column.virtual?).to be false
      expect(column.auto_increment?).to be false
      expect(column.name).to eq('total')
      expect(column.type).to eq(:integer)
    end

    it 'handles regular columns with defaults' do
      metadata = fresh_metadata(sql_type: 'VARCHAR', type: :string)
      column = build_column('status', 'active', metadata, true)

      expect(column.name).to eq('status')
      expect(column.type).to eq(:string)
      expect(column.default).to eq('active')
      expect(column.has_default?).to be true
      expect(column.virtual?).to be false
      expect(column.auto_increment?).to be false
    end
  end

  describe 'edge cases' do
    it 'handles columns with regular constraints' do
      metadata = fresh_metadata
      column = build_column('complex', nil, metadata, false)

      expect(column.virtual?).to be false
      expect(column.auto_increment?).to be false
    end

    it 'does not match partial words in extra' do
      metadata = fresh_metadata
      column = build_column(
        'partial',
        nil,
        metadata,
        true,
        extra: 'GENERATION failed VIRTUALLY impossible'
      )

      expect(column.virtual?).to be false
    end

    it 'handles unknown keyword arguments gracefully' do
      metadata = fresh_metadata
      expect do
        build_column('test', nil, metadata, true, unknown_param: 'value')
      end.not_to raise_error
    end

    it 'handles whitespace-only extra string' do
      metadata = fresh_metadata
      column = build_column('whitespace', nil, metadata, true, extra: '   ')
      expect(column.virtual?).to be false
    end
  end

  describe 'column reflection integration' do
    # Tests that connection.columns() correctly reflects column types after table creation
    # This verifies the full stack: create_table -> column_definitions -> parse_type_info -> Column

    let(:config) { { adapter: 'duckdb', database: ':memory:' } }

    before do
      ActiveRecord::Base.establish_connection(config)
      ActiveRecord::Base.connection.create_table(:column_reflection_test, id: false) do |t|
        # Standard types
        t.bigint :bigint_col
        t.integer :integer_col
        t.string :string_col
        t.boolean :boolean_col
        t.float :float_col
        t.decimal :decimal_col, precision: 10, scale: 2
        t.datetime :datetime_col
        t.date :date_col
        t.binary :binary_col

        # DuckDB signed integers
        t.tinyint :tinyint_col
        t.smallint :smallint_col

        # DuckDB unsigned integers
        t.utinyint :utinyint_col
        t.usmallint :usmallint_col
        t.uinteger :uinteger_col
        t.ubigint :ubigint_col

        # Other DuckDB types
        t.interval :interval_col
        t.uuid :uuid_col
      end
    end

    after do
      ActiveRecord::Base.connection.drop_table(:column_reflection_test, if_exists: true)
      ActiveRecord::Base.remove_connection
    end

    let(:connection) { ActiveRecord::Base.connection }
    let(:columns) { connection.columns(:column_reflection_test) }
    let(:columns_by_name) { columns.index_by(&:name) }

    # Verify SQL types are preserved correctly after schema reflection
    {
      'bigint_col' => 'BIGINT',
      'integer_col' => 'INTEGER',
      'string_col' => 'VARCHAR',
      'boolean_col' => 'BOOLEAN',
      'tinyint_col' => 'TINYINT',
      'smallint_col' => 'SMALLINT',
      'utinyint_col' => 'UTINYINT',
      'usmallint_col' => 'USMALLINT',
      'uinteger_col' => 'UINTEGER',
      'ubigint_col' => 'UBIGINT',
      'interval_col' => 'INTERVAL',
      'uuid_col' => 'UUID'
    }.each do |col_name, expected_sql_type|
      it "reflects #{expected_sql_type} type for #{col_name}" do
        col = columns_by_name[col_name]
        expect(col).not_to be_nil, "Column '#{col_name}' not found"
        expect(col.sql_type.upcase).to eq(expected_sql_type)
      end
    end

    it 'reflects DECIMAL with precision and scale' do
      col = columns_by_name['decimal_col']
      expect(col.sql_type.upcase).to match(/DECIMAL\(10,\s*2\)/i)
    end

    it 'reflects TIMESTAMP for datetime' do
      col = columns_by_name['datetime_col']
      expect(col.sql_type.upcase).to include('TIMESTAMP')
    end

    it 'reflects DATE type' do
      col = columns_by_name['date_col']
      expect(col.sql_type.upcase).to eq('DATE')
    end

    it 'reflects BLOB for binary' do
      col = columns_by_name['binary_col']
      expect(col.sql_type.upcase).to eq('BLOB')
    end

    it 'reflects REAL/FLOAT for float' do
      col = columns_by_name['float_col']
      expect(col.sql_type.upcase).to match(/REAL|FLOAT/)
    end
  end
end
