# frozen_string_literal: true

require 'spec_helper'

# Test suite for verifying sequence default handling in DuckDB tables with references
#
# This spec ensures that sequence defaults (nextval() calls) are only applied to
# primary key columns and not to reference columns or other non-primary key columns.
# This prevents the bug where sequence defaults were incorrectly applied to all columns.
#
# @see ActiveRecord::ConnectionAdapters::Duckdb::SchemaStatements#create_table
# @see ActiveRecord::ConnectionAdapters::Duckdb::SchemaCreation#add_column_options!
# rubocop:disable RSpec/DescribeClass
RSpec.describe 'References Sequence Fix' do
  let(:config) do
    {
      adapter: 'duckdb',
      database: ':memory:'
    }
  end

  let(:adapter) { ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(nil, nil, {}, config) }
  let(:test_tables) { %w[notes users posts categories uuid_table sequence_test regression_test] }

  before { adapter.send(:connect) }
  after { adapter.disconnect }

  # Retrieves column information for a given table using PRAGMA table_info
  #
  # @param table_name [String] The name of the table to inspect
  # @return [Array<Array>] Array of column information arrays where each inner array contains:
  #   [index, name, type, not_null, default_value, primary_key]
  def table_columns(table_name)
    adapter.execute("PRAGMA table_info('#{table_name}')").to_a
  end

  # Filters columns to find those with sequence defaults (nextval() calls)
  #
  # @param columns [Array<Array>] Column information arrays from table_columns
  # @return [Array<Array>] Columns that have sequence defaults in their default_value (index 4)
  def sequence_columns(columns)
    columns.select { |col| col[4]&.include?('nextval') }
  end

  # Finds the primary key column from a set of columns
  #
  # @param columns [Array<Array>] Column information arrays from table_columns
  # @return [Array, nil] The primary key column array or nil if not found
  def primary_key_column(columns)
    columns.find { |col| col[5] == true }
  end

  # Cleans up all test tables to ensure clean state
  #
  # @return [void]
  def cleanup_tables
    test_tables.each { |table| adapter.drop_table(table, if_exists: true) }
  end

  describe 'sequence defaults with references' do
    before { cleanup_tables }
    after { cleanup_tables }

    it 'applies sequence defaults only to primary key columns' do
      # This test verifies the core fix: sequence defaults should only be applied to primary keys
      # Create users table (baseline case)
      adapter.create_table(:users) do |t|
        t.string :name, null: false
        t.string :email, null: false
        t.timestamps
      end

      # Create notes table with references (problematic case before fix)
      adapter.create_table(:notes) do |t|
        t.text :content, null: false
        t.references :user, null: false, foreign_key: true
        t.uuid :uuid, null: false
        t.decimal :score, precision: 10, scale: 2
        t.timestamps
      end

      # Verify users table structure
      users_columns = table_columns('users')
      users_pk = primary_key_column(users_columns)
      users_sequences = sequence_columns(users_columns)

      expect(users_pk[1]).to eq('id')
      expect(users_sequences.length).to eq(1)
      expect(users_sequences.first[4]).to include('users_id_seq')

      # Verify notes table structure
      notes_columns = table_columns('notes')
      notes_pk = primary_key_column(notes_columns)
      notes_sequences = sequence_columns(notes_columns)

      expect(notes_pk[1]).to eq('id')
      expect(notes_sequences.length).to eq(1)
      expect(notes_sequences.first[4]).to include('notes_id_seq')

      # Verify non-primary key columns don't have sequence defaults
      non_pk_columns = notes_columns.reject { |col| col[5] == true }
      non_pk_columns.each do |col|
        expect(col[4]).not_to include('nextval') if col[4]
      end
    end

    it 'allows successful record creation with references' do
      # This test ensures that tables with references can be created and used successfully
      # Establish connection for ActiveRecord models
      ActiveRecord::Base.establish_connection(config)

      # Create tables using ActiveRecord's connection
      ActiveRecord::Base.connection.create_table(:users) do |t|
        t.string :name, null: false
        t.string :email, null: false
        t.timestamps
      end

      ActiveRecord::Base.connection.create_table(:notes) do |t|
        t.text :content, null: false
        t.references :user, null: false, foreign_key: true
        t.uuid :uuid, null: false
        t.timestamps
      end

      # Define test models with proper connection
      user_class = Class.new(ActiveRecord::Base) do
        self.table_name = 'users'
        validates :name, :email, presence: true
      end

      note_class = Class.new(ActiveRecord::Base) do
        self.table_name = 'notes'
        belongs_to :user, class_name: user_class.name
        validates :content, :uuid, presence: true
      end

      # Test record creation
      user = user_class.create!(
        name: 'Test User',
        email: 'test@example.com'
      )

      expect(user.id).to be_present
      expect(user.id).to be_a(Integer)

      note = note_class.create!(
        content: 'Test note content',
        user_id: user.id,
        uuid: SecureRandom.uuid
      )

      expect(note.id).to be_present
      expect(note.id).to be_a(Integer)
      expect(note.user_id).to eq(user.id)
      expect(note.uuid).to be_present
    ensure
      ActiveRecord::Base.remove_connection
    end

    it 'handles multiple tables with references correctly' do
      # This test verifies complex scenarios with multiple references and column types
      # Create a more complex scenario with multiple references
      adapter.create_table(:users) do |t|
        t.string :name, null: false
        t.timestamps
      end

      adapter.create_table(:categories) do |t|
        t.string :name, null: false
        t.timestamps
      end

      adapter.create_table(:posts) do |t|
        t.string :title, null: false
        t.text :body
        t.references :user, null: false, foreign_key: true
        t.references :category, null: true, foreign_key: true
        t.uuid :uuid, null: false
        t.decimal :rating, precision: 3, scale: 2
        t.boolean :published, default: false
        t.timestamps
      end

      # Verify only primary keys have sequence defaults
      posts_columns = table_columns('posts')
      posts_sequences = sequence_columns(posts_columns)
      posts_pk = primary_key_column(posts_columns)

      # Only the primary key should have a sequence default
      expect(posts_sequences.length).to eq(1)
      expect(posts_pk[1]).to eq('id')
      expect(posts_pk[4]).to include('posts_id_seq')

      # Verify specific non-primary columns don't have sequence defaults
      reference_column_names = %w[user_id category_id uuid rating published]
      reference_columns = posts_columns.select { |col| reference_column_names.include?(col[1]) }
      reference_columns.each do |col|
        expect(col[4]).not_to include('nextval') if col[4]
      end
    end

    it 'handles different primary key types correctly' do
      # This test ensures non-integer primary keys (like UUID) don't get sequence defaults
      # Test with UUID primary key
      adapter.create_table(:uuid_table, id: :uuid) do |t|
        t.string :name
        t.references :user, null: false
        t.timestamps
      end

      uuid_columns = table_columns('uuid_table')
      uuid_sequences = sequence_columns(uuid_columns)
      uuid_pk = primary_key_column(uuid_columns)

      # UUID primary key should NOT have sequence default
      expect(uuid_sequences.length).to eq(0)
      expect(uuid_pk[1]).to eq('id')
      expect(uuid_pk[4]).not_to include('nextval') if uuid_pk[4]

      # Verify no columns have sequence defaults
      uuid_columns.each do |col|
        expect(col[4]).not_to include('nextval') if col[4]
      end
    end

    it 'preserves sequence functionality for integer primary keys' do
      # This test verifies that sequence auto-increment still works for primary keys
      adapter.create_table(:sequence_test) do |t|
        t.string :name
        t.references :user, null: false
        t.timestamps
      end

      # Create multiple records to test sequence increment using proper timestamps
      current_time = Time.current.strftime('%Y-%m-%d %H:%M:%S')

      result1 = adapter.execute("INSERT INTO sequence_test (name, user_id, created_at, updated_at) VALUES ('test1', 1, '#{current_time}', '#{current_time}') RETURNING id")
      result2 = adapter.execute("INSERT INTO sequence_test (name, user_id, created_at, updated_at) VALUES ('test2', 1, '#{current_time}', '#{current_time}') RETURNING id")

      # Convert results to arrays and extract IDs
      result1_array = result1.to_a
      result2_array = result2.to_a

      id1 = result1_array.first[0]
      id2 = result2_array.first[0]

      expect(id1).to be_a(Integer)
      expect(id2).to be_a(Integer)
      expect(id2).to be > id1
    end
  end

  describe 'regression test for sequence injection bug' do
    it 'does not inject sequence defaults into non-primary key columns' do
      # Regression test for the specific bug where sequence defaults were applied to all columns
      # This specifically tests the bug that was fixed where the regex pattern
      # was too greedy and applied sequence defaults to ALL columns
      adapter.create_table(:regression_test) do |t|
        t.string :title
        t.references :user, null: false
        t.references :category, null: true
        t.uuid :external_id
        t.decimal :amount, precision: 10, scale: 2
        t.boolean :active, default: true
        t.timestamps
      end

      columns = table_columns('regression_test')
      test_sequences = sequence_columns(columns)
      test_pk = primary_key_column(columns)

      # Should be exactly 1 sequence default (only the primary key)
      expect(test_sequences.length).to eq(1)

      # Verify the primary key is the one with the sequence
      expect(test_pk).not_to be_nil
      expect(test_pk[1]).to eq('id')
      expect(test_pk[4]).to include('nextval')
      expect(test_pk[4]).to include('regression_test_id_seq')

      # Verify specific columns don't have sequence defaults
      reference_column_names = %w[user_id category_id external_id amount]
      reference_columns = columns.select { |col| reference_column_names.include?(col[1]) }

      reference_columns.each do |col|
        expect(col[4]).not_to include('nextval') if col[4]
      end
    end
  end
end
# rubocop:enable RSpec/DescribeClass
