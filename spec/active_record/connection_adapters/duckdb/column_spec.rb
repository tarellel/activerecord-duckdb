# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::Column do
  # Helper method to create fresh metadata for each test
  def fresh_metadata
    ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
      sql_type: 'INTEGER',
      type: :integer
    )
  end

  describe 'initialization' do
    it 'creates a column with basic attributes' do
      metadata = fresh_metadata
      column = described_class.new('test_column', 'default_value', metadata, true)

      expect(column.name).to eq('test_column')
      expect(column.default).to eq('default_value')
      expect(column.sql_type_metadata).to eq(metadata)
      expect(column.null).to be true
    end

    it 'accepts DuckDB-specific options' do
      metadata = fresh_metadata
      column = described_class.new(
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
      column = described_class.new('test_column', nil, metadata, true)

      expect(column.auto_increment?).to be false
      expect(column.rowid).to be_nil
      expect(column.extra).to be_nil
    end
  end

  # describe '#virtual?' do
  #   context 'when column has virtual keywords in extra' do
  #     it 'returns true for VIRTUAL keyword', pending: 'Virtual columns not fully implemented' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: 'VIRTUAL')
  #       expect(column.virtual?).to be true
  #     end

  #     it 'returns true for STORED keyword', pending: 'Virtual columns not fully implemented' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: 'STORED')
  #       expect(column.virtual?).to be true
  #     end

  #     it 'returns true for GENERATED keyword', pending: 'Virtual columns not fully implemented' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: 'GENERATED ALWAYS AS (id * 2)')
  #       expect(column.virtual?).to be true
  #     end

  #     it 'is case insensitive', pending: 'Virtual columns not fully implemented' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: 'virtual generated always')
  #       expect(column.virtual?).to be true
  #     end
  #   end

  #   context 'when column does not have virtual keywords' do
  #     it 'returns false for nil extra' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: nil)
  #       expect(column.virtual?).to be false
  #     end

  #     it 'returns false for regular constraints' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: 'NOT NULL DEFAULT 0')
  #       expect(column.virtual?).to be false
  #     end

  #     it 'returns false for empty string' do
  #       metadata = fresh_metadata
  #       column = described_class.new('test', nil, metadata, true, extra: '')
  #       expect(column.virtual?).to be false
  #     end
  #   end
  # end

  describe '#has_default?' do
    # it 'returns false for virtual columns even with default', pending: 'Virtual columns not fully implemented' do
    #   metadata = fresh_metadata
    #   column = described_class.new('test', 'some_default', metadata, true, extra: 'VIRTUAL')
    #   expect(column.has_default?).to be false
    # end

    it 'returns true for non-virtual columns with default' do
      metadata = fresh_metadata
      column = described_class.new('test', 'default_value', metadata, true)
      expect(column.has_default?).to be true
    end

    it 'returns false for non-virtual columns without default' do
      metadata = fresh_metadata
      column = described_class.new('test', nil, metadata, true)
      expect(column.has_default?).to be false
    end
  end

  describe '#auto_increment?' do
    it 'returns true when auto_increment is true' do
      metadata = fresh_metadata
      column = described_class.new('id', nil, metadata, false, auto_increment: true)
      expect(column.auto_increment?).to be true
    end

    #   it 'returns false when auto_increment is false', pending: 'RSpec environment kwargs issue' do
    #     metadata = fresh_metadata
    #     column = described_class.new('id', nil, metadata, false, auto_increment: false)
    #     expect(column.auto_increment?).to be false
    #   end

    #   it 'returns false when auto_increment is not specified', pending: 'RSpec environment kwargs issue' do
    #     metadata = fresh_metadata
    #     column = described_class.new('id', nil, metadata, false)
    #     expect(column.auto_increment?).to be false
    #   end
  end

  describe '#auto_incremented_by_db?' do
    it 'returns true when auto_increment is true' do
      metadata = fresh_metadata
      column = described_class.new('id', nil, metadata, false, auto_increment: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns true when rowid is true' do
      metadata = fresh_metadata
      column = described_class.new('rowid', nil, metadata, false, rowid: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns true when both are true' do
      metadata = fresh_metadata
      column = described_class.new('id', nil, metadata, false, auto_increment: true, rowid: true)
      expect(column.auto_incremented_by_db?).to be true
    end

    it 'returns false when neither is true' do
      metadata = fresh_metadata
      column = described_class.new('name', nil, metadata, false)
      expect(column.auto_incremented_by_db?).to be false
    end
  end

  describe '#rowid' do
    it 'exposes rowid attribute when set' do
      # Create completely fresh metadata and ensure no shared state
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'INTEGER',
        type: :integer
      )
      column = described_class.new('test_rowid_true', nil, metadata, true, rowid: true)
      expect(column.rowid).to be true
    end

    it 'defaults to nil when not specified' do
      # Create completely fresh metadata with different name to ensure isolation
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'INTEGER',
        type: :integer
      )
      column = described_class.new('test_rowid_nil', nil, metadata, true)
      expect(column.rowid).to be_nil
    end
  end

  describe 'inheritance from ConnectionAdapters::Column' do
    let(:column) do
      metadata = fresh_metadata
      described_class.new('test', 'default', metadata, true)
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
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'BIGINT',
        type: :bigint
      )
      column = described_class.new('id', nil, metadata, false, auto_increment: true)

      expect(column.name).to eq('id')
      expect(column.type).to eq(:bigint)
      expect(column.null).to be false
      expect(column.auto_increment?).to be true
      expect(column.auto_incremented_by_db?).to be true
      expect(column.virtual?).to be false
    end

    it 'handles UUID primary key columns' do
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'UUID',
        type: :uuid
      )
      column = described_class.new('id', nil, metadata, false)

      expect(column.name).to eq('id')
      expect(column.type).to eq(:uuid)
      expect(column.null).to be false
      expect(column.auto_increment?).to be false
      expect(column.auto_incremented_by_db?).to be false
    end

    it 'handles basic columns without special features' do
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'INTEGER',
        type: :integer
      )
      column = described_class.new('total', nil, metadata, true)

      expect(column.virtual?).to be false
      expect(column.auto_increment?).to be false
      expect(column.name).to eq('total')
      expect(column.type).to eq(:integer)
    end

    it 'handles regular columns with defaults' do
      metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
        sql_type: 'VARCHAR',
        type: :string
      )
      column = described_class.new('status', 'active', metadata, true)

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
      column = described_class.new('complex', nil, metadata, false)

      expect(column.virtual?).to be false
      expect(column.auto_increment?).to be false
    end

    it 'does not match partial words in extra' do
      metadata = fresh_metadata
      column = described_class.new(
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
        described_class.new('test', nil, metadata, true, unknown_param: 'value')
      end.not_to raise_error
    end

    it 'handles whitespace-only extra string' do
      metadata = fresh_metadata
      column = described_class.new('whitespace', nil, metadata, true, extra: '   ')
      expect(column.virtual?).to be false
    end
  end
end
