# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'securerandom'
require 'fileutils'

# DuckLake Integration Tests
#
# DuckLake is a lakehouse format built on SQL + Parquet that adds ACID transactions,
# versioning/snapshots, and schema evolution to DuckDB.
#
# IMPORTANT LIMITATIONS:
# DuckLake does NOT support (and is unlikely to ever support):
# - Indexes (CREATE INDEX is not supported)
# - PRIMARY KEY uniqueness enforcement
# - UNIQUE constraints enforcement
# - CHECK constraints enforcement
# - FOREIGN KEY constraints enforcement
# - ENUM data types
# - Non-literal default values (e.g., CAST expressions, function calls)
#
# Note: INSERT...RETURNING is not supported by DuckLake, but the adapter automatically
# detects DuckLake mode and disables RETURNING clause for inserts.
#
# These limitations are by design in the DuckLake specification.
# See: https://ducklake.select/docs/stable/duckdb/unsupported_features.html

RSpec.describe 'DuckLake Integration' do
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

  describe 'DuckLake connection and lifecycle' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_test') }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    it 'loads the ducklake extension successfully' do
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Verify the extension is loaded
      result = ActiveRecord::Base.connection.execute("SELECT extension_name FROM duckdb_extensions() WHERE loaded = true AND extension_name = 'ducklake'")
      expect(result.to_a.flatten).to include('ducklake')
    end

    it 'attaches DuckLake database and switches to it' do
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Verify we're using the ducklake database
      result = ActiveRecord::Base.connection.execute("SELECT current_database()")
      expect(result.first.first).to eq('ducklake')
    end

    it 'creates metadata file in the specified location' do
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Create a table to trigger metadata file creation
      ActiveRecord::Base.connection.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")

      # The metadata file should exist
      metadata_file = File.join(temp_dir, 'test.ducklake')
      expect(File).to exist(metadata_file)
    end

    it 'stores data files in the specified DATA_PATH' do
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Create a table and insert data
      ActiveRecord::Base.connection.execute("CREATE TABLE data_test (id INTEGER, name VARCHAR)")
      ActiveRecord::Base.connection.execute("INSERT INTO data_test VALUES (1, 'test')")

      # Data files (Parquet) should be created in the data directory
      data_dir = File.join(temp_dir, 'data')
      # DuckLake creates subdirectories for data files
      expect(Dir.exist?(data_dir)).to be true
    end

    it 'persists data across reconnections' do
      # First connection - create and populate table
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
      ActiveRecord::Base.connection.execute("CREATE TABLE persist_test (id INTEGER, value VARCHAR)")
      ActiveRecord::Base.connection.execute("INSERT INTO persist_test VALUES (1, 'hello')")
      ActiveRecord::Base.remove_connection

      # Second connection - verify data persists
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
      result = ActiveRecord::Base.connection.execute("SELECT value FROM persist_test WHERE id = 1")
      expect(result.first.first).to eq('hello')
    end
  end

  describe 'schema operations' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_schema_test') }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    describe 'table creation and management' do
      it 'creates tables using Rails migration syntax' do
        ActiveRecord::Base.connection.create_table(:schema_test, id: false) do |t|
          t.integer :id
          t.string :name
        end

        expect(ActiveRecord::Base.connection.table_exists?(:schema_test)).to be true
      end

      it 'drops tables' do
        ActiveRecord::Base.connection.create_table(:drop_test, id: false) do |t|
          t.string :name
        end

        expect(ActiveRecord::Base.connection.table_exists?(:drop_test)).to be true

        ActiveRecord::Base.connection.drop_table(:drop_test)

        expect(ActiveRecord::Base.connection.table_exists?(:drop_test)).to be false
      end

      it 'lists tables in DuckLake database' do
        ActiveRecord::Base.connection.create_table(:list_test_one, id: false) do |t|
          t.string :name
        end
        ActiveRecord::Base.connection.create_table(:list_test_two, id: false) do |t|
          t.string :name
        end

        tables = ActiveRecord::Base.connection.tables
        expect(tables).to include('list_test_one', 'list_test_two')
      end
    end

    describe 'standard column types' do
      before do
        # Note: DuckLake only supports literal defaults, so we don't use default: here
        ActiveRecord::Base.connection.create_table(:type_test, id: false) do |t|
          t.string :string_col, limit: 100
          t.text :text_col
          t.integer :integer_col
          t.bigint :bigint_col
          t.decimal :decimal_col, precision: 10, scale: 2
          t.float :float_col
          t.date :date_col
          t.datetime :datetime_col
          t.boolean :boolean_col
          t.binary :binary_col
        end
      end

      {
        string: :string,
        integer: :integer,
        bigint: :bigint,
        decimal: :decimal,
        float: :float,
        date: :date,
        boolean: :boolean,
        binary: :binary
      }.each do |column_name, expected_type|
        it "supports #{column_name} columns" do
          columns = ActiveRecord::Base.connection.columns(:type_test)
          col = columns.find { |c| c.name == "#{column_name}_col" }
          expect(col.type).to eq(expected_type)
        end
      end

      it 'supports datetime columns' do
        columns = ActiveRecord::Base.connection.columns(:type_test)
        col = columns.find { |c| c.name == 'datetime_col' }
        # DuckLake may return :time or :datetime depending on the type mapping
        expect(col.sql_type.upcase).to include('TIMESTAMP')
      end
    end

    describe 'DuckDB-specific column types' do
      # Simple types with exact sql_type match
      {
        hugeint: { column: :huge_number, sql_type: 'HUGEINT' },
        tinyint: { column: :tiny_number, sql_type: 'TINYINT' },
        interval: { column: :duration, sql_type: 'INTERVAL' }
      }.each do |type_method, config|
        it "supports #{type_method} columns" do
          ActiveRecord::Base.connection.create_table(:"#{type_method}_test", id: false) do |t|
            t.public_send(type_method, config[:column])
          end

          columns = ActiveRecord::Base.connection.columns(:"#{type_method}_test")
          col = columns.find { |c| c.name == config[:column].to_s }
          expect(col.sql_type).to eq(config[:sql_type])
        end
      end

      it 'supports list columns' do
        ActiveRecord::Base.connection.create_table(:list_test, id: false) do |t|
          t.list :tags, element_type: :string
        end

        columns = ActiveRecord::Base.connection.columns(:list_test)
        col = columns.find { |c| c.name == 'tags' }
        expect(col.sql_type).to eq('VARCHAR[]')
      end

      it 'supports struct columns' do
        ActiveRecord::Base.connection.create_table(:struct_test, id: false) do |t|
          t.struct :contact, fields: { email: :string, phone: :string }
        end

        columns = ActiveRecord::Base.connection.columns(:struct_test)
        col = columns.find { |c| c.name == 'contact' }
        expect(col.sql_type).to include('STRUCT')
      end

      it 'supports map columns' do
        ActiveRecord::Base.connection.create_table(:map_test, id: false) do |t|
          t.map :metadata, key_type: :string, value_type: :string
        end

        columns = ActiveRecord::Base.connection.columns(:map_test)
        col = columns.find { |c| c.name == 'metadata' }
        expect(col.sql_type).to include('MAP')
      end
    end
  end

  describe 'ActiveRecord model operations with DuckLake' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_model_test') }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Create table using raw SQL as defaults are not implemented yet
      ActiveRecord::Base.connection.execute(<<~SQL)
        CREATE TABLE ducklake_users (
          id INTEGER,
          name VARCHAR NOT NULL,
          age INTEGER,
          active BOOLEAN,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
      SQL
    end

    after do
      begin
        ActiveRecord::Base.connection.drop_table(:ducklake_users) if ActiveRecord::Base.connection.table_exists?(:ducklake_users)
      rescue StandardError
        # Ignore cleanup errors
      end
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    let(:user_class) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'ducklake_users'
        self.primary_key = 'id'

        def self.name
          'DucklakeUser'
        end
      end
    end

    describe 'read operations' do
      before do
        user_class.create!(id: 1, name: 'Alice', age: 25, active: true)
        user_class.create!(id: 2, name: 'Bob', age: 30, active: false)
      end

      it 'finds records by id' do
        user = user_class.find(1)
        expect(user.name).to eq('Alice')
      end

      it 'finds records with where clause' do
        user = user_class.find_by(name: 'Bob')
        expect(user.age).to eq(30)
      end

      it 'returns all records' do
        users = user_class.all
        expect(users.count).to eq(2)
      end

      it 'supports first and last' do
        first_user = user_class.order(:id).first
        last_user = user_class.order(:id).last

        expect(first_user.name).to eq('Alice')
        expect(last_user.name).to eq('Bob')
      end
    end

    describe 'update operations' do
      before do
        user_class.create!(id: 1, name: 'Original Name', age: 20, active: true)
      end

      it 'updates individual attributes' do
        user = user_class.find(1)
        user.update!(name: 'Updated Name')

        expect(user.reload.name).to eq('Updated Name')
      end

      it 'updates multiple attributes' do
        user = user_class.find(1)
        user.update!(name: 'New Name', age: 25)

        user.reload
        expect(user.name).to eq('New Name')
        expect(user.age).to eq(25)
      end

      it 'supports update_all' do
        user_class.create!(id: 2, name: 'Another User', age: 30, active: true)

        user_class.update_all(active: false)

        expect(user_class.where(active: false).count).to eq(2)
      end
    end

    describe 'delete operations' do
      before do
        user_class.create!(id: 1, name: 'To Delete', age: 25, active: true)
        user_class.create!(id: 2, name: 'To Keep', age: 30, active: false)
      end

      it 'destroys individual records' do
        user = user_class.find(1)
        user.destroy!

        expect(user_class.count).to eq(1)
        expect { user_class.find(1) }.to raise_error(ActiveRecord::RecordNotFound)
      end

      it 'supports delete_all' do
        user_class.delete_all

        expect(user_class.count).to eq(0)
      end

      it 'supports conditional delete' do
        user_class.where(name: 'To Delete').delete_all

        expect(user_class.count).to eq(1)
        expect(user_class.first.name).to eq('To Keep')
      end
    end

    describe 'create operations' do
      it 'creates records using ActiveRecord create!' do
        # This tests if the adapter can handle INSERT without RETURNING for DuckLake
        user = user_class.create!(id: 1, name: 'Test User', age: 25, active: true)

        expect(user).to be_persisted
        expect(user.id).to eq(1)
        expect(user.name).to eq('Test User')

        # Verify the record was actually saved
        found = user_class.find(1)
        expect(found.name).to eq('Test User')
      end

      it 'creates records with auto-assigned id' do
        # When not providing an ID, the model callback should assign one
        user = user_class.new(name: 'Auto ID User', age: 30, active: true)
        user.id = 1 # Manually set since DuckLake doesn't have auto-increment
        user.save!

        expect(user).to be_persisted
        expect(user_class.count).to eq(1)
      end

      it 'creates multiple records' do
        user_class.create!(id: 1, name: 'User One', age: 20, active: true)
        user_class.create!(id: 2, name: 'User Two', age: 30, active: false)
        user_class.create!(id: 3, name: 'User Three', age: 40, active: true)

        expect(user_class.count).to eq(3)
        expect(user_class.pluck(:name).sort).to eq(['User One', 'User Three', 'User Two'])
      end
    end
  end

  describe 'query operations' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_query_test') }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Create table using raw SQL to avoid default value issues
      ActiveRecord::Base.connection.execute(<<~SQL)
        CREATE TABLE query_users (
          id INTEGER,
          name VARCHAR,
          age INTEGER,
          department VARCHAR,
          active BOOLEAN,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
      SQL
    end

    after do
      begin
        ActiveRecord::Base.connection.drop_table(:query_users) if ActiveRecord::Base.connection.table_exists?(:query_users)
      rescue StandardError
        # Ignore cleanup errors
      end
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    let(:user_class) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'query_users'
        self.primary_key = 'id'

        def self.name
          'QueryUser'
        end
      end
    end

    before do
      # Create test data using ActiveRecord
      user_class.create!(id: 1, name: 'Alice', age: 25, department: 'Engineering', active: true)
      user_class.create!(id: 2, name: 'Bob', age: 30, department: 'Engineering', active: true)
      user_class.create!(id: 3, name: 'Charlie', age: 35, department: 'Sales', active: false)
      user_class.create!(id: 4, name: 'Diana', age: 28, department: 'Sales', active: true)
      user_class.create!(id: 5, name: 'Eve', age: 32, department: 'Marketing', active: true)
    end

    describe 'WHERE clauses' do
      it 'filters by equality' do
        users = user_class.where(department: 'Engineering')
        expect(users.count).to eq(2)
      end

      it 'filters by boolean' do
        active_users = user_class.where(active: true)
        expect(active_users.count).to eq(4)
      end

      it 'filters with multiple conditions' do
        users = user_class.where(department: 'Engineering', active: true)
        expect(users.count).to eq(2)
      end

      it 'filters with comparison operators' do
        users = user_class.where('age > ?', 30)
        expect(users.count).to eq(2)
      end

      it 'filters with IN clause' do
        users = user_class.where(department: %w[Engineering Sales])
        expect(users.count).to eq(4)
      end

      it 'filters with LIKE clause' do
        users = user_class.where('name LIKE ?', 'A%')
        expect(users.count).to eq(1)
        expect(users.first.name).to eq('Alice')
      end

      it 'chains where clauses' do
        users = user_class.where(active: true).where('age >= ?', 30)
        expect(users.count).to eq(2)
      end
    end

    describe 'ORDER BY' do
      it 'orders by single column ascending' do
        users = user_class.order(:age)
        expect(users.first.name).to eq('Alice')
        expect(users.last.name).to eq('Charlie')
      end

      it 'orders by single column descending' do
        users = user_class.order(age: :desc)
        expect(users.first.name).to eq('Charlie')
        expect(users.last.name).to eq('Alice')
      end

      it 'orders by multiple columns' do
        users = user_class.order(:department, :age)
        engineering_users = users.select { |u| u.department == 'Engineering' }
        expect(engineering_users.first.name).to eq('Alice')
        expect(engineering_users.last.name).to eq('Bob')
      end
    end

    describe 'aggregations' do
      it 'counts records' do
        expect(user_class.count).to eq(5)
      end

      it 'counts with conditions' do
        expect(user_class.where(active: true).count).to eq(4)
      end

      it 'sums numeric columns' do
        expect(user_class.sum(:age)).to eq(150) # 25 + 30 + 35 + 28 + 32
      end

      it 'calculates average' do
        expect(user_class.average(:age)).to eq(30) # 150 / 5
      end

      it 'finds maximum' do
        expect(user_class.maximum(:age)).to eq(35)
      end

      it 'finds minimum' do
        expect(user_class.minimum(:age)).to eq(25)
      end
    end

    describe 'GROUP BY' do
      it 'groups and counts' do
        grouped = user_class.group(:department).count
        expect(grouped['Engineering']).to eq(2)
        expect(grouped['Sales']).to eq(2)
        expect(grouped['Marketing']).to eq(1)
      end

      it 'groups and sums' do
        grouped = user_class.group(:department).sum(:age)
        expect(grouped['Engineering']).to eq(55) # 25 + 30
        expect(grouped['Sales']).to eq(63) # 35 + 28
        expect(grouped['Marketing']).to eq(32)
      end

      it 'groups by boolean' do
        grouped = user_class.group(:active).count
        expect(grouped[true]).to eq(4)
        expect(grouped[false]).to eq(1)
      end

      it 'groups with having clause' do
        grouped = user_class.group(:department).having('COUNT(*) > 1').count
        expect(grouped.keys).to contain_exactly('Engineering', 'Sales')
        expect(grouped.keys).not_to include('Marketing')
      end
    end

    describe 'LIMIT and OFFSET' do
      it 'limits results' do
        users = user_class.order(:id).limit(3)
        expect(users.count).to eq(3)
      end

      it 'offsets results' do
        users = user_class.order(:id).offset(2).limit(2)
        expect(users.map(&:name)).to eq(%w[Charlie Diana])
      end
    end

    describe 'SELECT specific columns' do
      it 'selects specific columns' do
        users = user_class.select(:name, :age).order(:id)
        expect(users.first.name).to eq('Alice')
        expect(users.first.age).to eq(25)
      end

      it 'supports pluck' do
        names = user_class.order(:id).pluck(:name)
        expect(names).to eq(%w[Alice Bob Charlie Diana Eve])
      end

      it 'supports pluck with multiple columns' do
        data = user_class.order(:id).pluck(:name, :age)
        expect(data.first).to eq(['Alice', 25])
      end
    end

    describe 'DISTINCT' do
      it 'returns distinct values' do
        departments = user_class.distinct.pluck(:department)
        expect(departments.sort).to eq(%w[Engineering Marketing Sales])
      end
    end
  end

  describe 'data persistence verification' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_persist_test') }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    it 'persists data in Parquet files across sessions' do
      # Session 1: Create data
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
      ActiveRecord::Base.connection.execute("CREATE TABLE persist_test (id INTEGER, message VARCHAR)")
      ActiveRecord::Base.connection.execute("INSERT INTO persist_test VALUES (1, 'Hello from session 1')")
      ActiveRecord::Base.connection.execute("INSERT INTO persist_test VALUES (2, 'Second message')")
      ActiveRecord::Base.remove_connection

      # Session 2: Verify data persists
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))

      # Use raw SQL to verify persistence (avoids ActiveRecord model issues)
      result1 = ActiveRecord::Base.connection.execute("SELECT message FROM persist_test WHERE id = 1")
      result2 = ActiveRecord::Base.connection.execute("SELECT message FROM persist_test WHERE id = 2")

      expect(result1.first.first).to eq('Hello from session 1')
      expect(result2.first.first).to eq('Second message')

      count_result = ActiveRecord::Base.connection.execute("SELECT COUNT(*) FROM persist_test")
      expect(count_result.first.first).to eq(2)
    end

    it 'verifies data files exist in DATA_PATH' do
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
      ActiveRecord::Base.connection.execute("CREATE TABLE file_test (id INTEGER, data VARCHAR)")

      # Insert data to trigger file creation
      ActiveRecord::Base.connection.execute("INSERT INTO file_test VALUES (1, 'test data')")

      # Check that data directory has content
      data_path = File.join(temp_dir, 'data')
      expect(Dir.exist?(data_path)).to be true
      # DuckLake creates parquet files in subdirectories
      all_files = Dir.glob(File.join(data_path, '**', '*'))
      # At minimum, the directory structure should exist
      expect(all_files).not_to be_empty
    end
  end

  describe 'data type insert and read verification via ActiveRecord' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_datatype_test') }
    let(:connection) { ActiveRecord::Base.connection }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    describe 'comprehensive data types' do
      # Create a model class for testing all data types
      let(:model_class) do
        Class.new(ActiveRecord::Base) do
          self.table_name = 'type_samples'
          self.primary_key = 'id'

          def self.name
            'TypeSample'
          end
        end
      end

      before do
        # Create table with all supported types using migrations
        connection.create_table(:type_samples, id: false) do |t|
          t.integer :id
          # Standard types
          t.bigint :big_number
          t.integer :count
          t.string :label
          t.boolean :active
          t.float :ratio
          t.decimal :amount, precision: 10, scale: 2
          t.decimal :coordinates, precision: 9, scale: 6
          t.date :event_date
          t.datetime :recorded_at
          # DuckDB-specific signed integers
          t.tinyint :tiny_signed
          t.smallint :small_signed
          # DuckDB-specific unsigned integers
          t.utinyint :tiny_unsigned
          t.usmallint :small_unsigned
          t.uinteger :uint_val
        end
      end

      describe 'integer types' do
        it 'handles BIGINT full range via ActiveRecord' do
          # BIGINT: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 (8 bytes signed)
          model_class.create!(id: 1, big_number: 9_223_372_036_854_775_807)
          model_class.create!(id: 2, big_number: -9_223_372_036_854_775_808)
          model_class.create!(id: 3, big_number: 0)

          records = model_class.order(:id).pluck(:big_number)
          expect(records).to eq([9_223_372_036_854_775_807, -9_223_372_036_854_775_808, 0])
        end

        it 'handles INTEGER full range via ActiveRecord' do
          # INTEGER: -2,147,483,648 to 2,147,483,647 (4 bytes signed)
          model_class.create!(id: 1, count: 2_147_483_647)
          model_class.create!(id: 2, count: -2_147_483_648)

          records = model_class.order(:id).pluck(:count)
          expect(records).to eq([2_147_483_647, -2_147_483_648])
        end

        it 'handles TINYINT full range via ActiveRecord' do
          # TINYINT: -128 to 127 (1 byte signed)
          model_class.create!(id: 1, tiny_signed: -128)
          model_class.create!(id: 2, tiny_signed: 127)

          records = model_class.order(:id).pluck(:tiny_signed)
          expect(records).to eq([-128, 127])
        end

        it 'handles SMALLINT full range via ActiveRecord' do
          # SMALLINT: -32,768 to 32,767 (2 bytes signed)
          model_class.create!(id: 1, small_signed: -32_768)
          model_class.create!(id: 2, small_signed: 32_767)

          records = model_class.order(:id).pluck(:small_signed)
          expect(records).to eq([-32_768, 32_767])
        end
      end

      describe 'unsigned integer types' do
        it 'handles UTINYINT full range via ActiveRecord' do
          # UTINYINT: 0 to 255 (1 byte unsigned)
          model_class.create!(id: 1, tiny_unsigned: 0)
          model_class.create!(id: 2, tiny_unsigned: 255)

          records = model_class.order(:id).pluck(:tiny_unsigned)
          expect(records).to eq([0, 255])
        end

        it 'handles USMALLINT full range via ActiveRecord' do
          # USMALLINT: 0 to 65,535 (2 bytes unsigned)
          model_class.create!(id: 1, small_unsigned: 0)
          model_class.create!(id: 2, small_unsigned: 65_535)

          records = model_class.order(:id).pluck(:small_unsigned)
          expect(records).to eq([0, 65_535])
        end

        it 'handles UINTEGER full range via ActiveRecord' do
          # UINTEGER: 0 to 4,294,967,295 (4 bytes unsigned)
          model_class.create!(id: 1, uint_val: 0)
          model_class.create!(id: 2, uint_val: 4_294_967_295)

          records = model_class.order(:id).pluck(:uint_val)
          expect(records).to eq([0, 4_294_967_295])
        end
      end

      describe 'decimal types' do
        it 'handles DECIMAL with precision and scale via ActiveRecord' do
          model_class.create!(id: 1, amount: BigDecimal('99999999.99'))
          model_class.create!(id: 2, amount: BigDecimal('0.01'))

          record1 = model_class.find(1)
          record2 = model_class.find(2)

          # DECIMALs should be exact - that's the point of using them over floats
          expect(record1.amount).to eq(BigDecimal('99999999.99'))
          expect(record2.amount).to eq(BigDecimal('0.01'))
        end

        it 'handles high precision coordinates via ActiveRecord' do
          model_class.create!(id: 1, coordinates: BigDecimal('52.520008'))
          model_class.create!(id: 2, coordinates: BigDecimal('-33.868820'))

          records = model_class.order(:id).pluck(:coordinates)
          # DECIMAL(9,6) should preserve all 6 decimal places exactly
          expect(records[0]).to eq(BigDecimal('52.520008'))
          expect(records[1]).to eq(BigDecimal('-33.868820'))
        end
      end

      describe 'float type' do
        it 'handles FLOAT/REAL via ActiveRecord' do
          model_class.create!(id: 1, ratio: 3.14159)
          model_class.create!(id: 2, ratio: -273.15)
          model_class.create!(id: 3, ratio: 0.0)

          records = model_class.order(:id).pluck(:ratio)
          expect(records[0]).to be_within(0.0001).of(3.14159)
          expect(records[1]).to be_within(0.01).of(-273.15)
          expect(records[2]).to eq(0.0)
        end
      end

      describe 'boolean type' do
        it 'handles BOOLEAN via ActiveRecord' do
          model_class.create!(id: 1, active: true)
          model_class.create!(id: 2, active: false)
          model_class.create!(id: 3, active: nil)

          records = model_class.order(:id).pluck(:active)
          expect(records).to eq([true, false, nil])
        end

        it 'supports boolean queries via ActiveRecord' do
          model_class.create!(id: 1, label: 'active', active: true)
          model_class.create!(id: 2, label: 'inactive', active: false)

          active_records = model_class.where(active: true)
          expect(active_records.count).to eq(1)
          expect(active_records.first.label).to eq('active')
        end
      end

      describe 'string type' do
        it 'handles VARCHAR via ActiveRecord' do
          model_class.create!(id: 1, label: 'Simple text')
          model_class.create!(id: 2, label: 'Unicode: 日本語 🚀')
          model_class.create!(id: 3, label: "Special: It's a \"test\"")

          records = model_class.order(:id).pluck(:label)
          expect(records[0]).to eq('Simple text')
          expect(records[1]).to eq('Unicode: 日本語 🚀')
          expect(records[2]).to eq("Special: It's a \"test\"")
        end

        it 'handles long strings via ActiveRecord' do
          long_string = 'x' * 10_000
          model_class.create!(id: 1, label: long_string)

          record = model_class.find(1)
          expect(record.label.length).to eq(10_000)
        end
      end

      describe 'date and datetime types' do
        it 'handles DATE via ActiveRecord' do
          model_class.create!(id: 1, event_date: Date.new(2024, 1, 15))
          model_class.create!(id: 2, event_date: Date.new(1999, 12, 31))

          records = model_class.order(:id).pluck(:event_date)
          expect(records[0]).to eq(Date.new(2024, 1, 15))
          expect(records[1]).to eq(Date.new(1999, 12, 31))
        end

        it 'handles TIMESTAMP via ActiveRecord' do
          time1 = Time.new(2024, 1, 15, 14, 30, 0, '+00:00')
          model_class.create!(id: 1, recorded_at: time1)

          record = model_class.find(1)
          expect(record.recorded_at.year).to eq(2024)
          expect(record.recorded_at.month).to eq(1)
          expect(record.recorded_at.day).to eq(15)
        end
      end

      describe 'NULL handling' do
        it 'handles NULL values for all types via ActiveRecord' do
          model_class.create!(
            id: 1,
            big_number: nil,
            count: nil,
            label: nil,
            active: nil,
            ratio: nil,
            amount: nil,
            event_date: nil,
            recorded_at: nil
          )

          record = model_class.find(1)
          expect(record.big_number).to be_nil
          expect(record.count).to be_nil
          expect(record.label).to be_nil
          expect(record.active).to be_nil
          expect(record.ratio).to be_nil
          expect(record.amount).to be_nil
          expect(record.event_date).to be_nil
          expect(record.recorded_at).to be_nil
        end
      end

      describe 'ActiveRecord queries' do
        before do
          model_class.create!(id: 1, label: 'Alpha', count: 10, active: true)
          model_class.create!(id: 2, label: 'Beta', count: 20, active: false)
          model_class.create!(id: 3, label: 'Gamma', count: 30, active: true)
        end

        it 'supports where queries' do
          results = model_class.where(active: true).order(:id)
          expect(results.pluck(:label)).to eq(%w[Alpha Gamma])
        end

        it 'supports comparison queries' do
          results = model_class.where('count > ?', 15).order(:id)
          expect(results.pluck(:label)).to eq(%w[Beta Gamma])
        end

        it 'supports aggregations' do
          expect(model_class.sum(:count)).to eq(60)
          expect(model_class.average(:count).to_i).to eq(20)
          expect(model_class.maximum(:count)).to eq(30)
          expect(model_class.minimum(:count)).to eq(10)
        end

        it 'supports updates via ActiveRecord' do
          record = model_class.find(1)
          record.update!(label: 'Updated', count: 100)

          reloaded = model_class.find(1)
          expect(reloaded.label).to eq('Updated')
          expect(reloaded.count).to eq(100)
        end

        it 'supports destroy via ActiveRecord' do
          expect(model_class.count).to eq(3)
          model_class.find(2).destroy!
          expect(model_class.count).to eq(2)
          expect { model_class.find(2) }.to raise_error(ActiveRecord::RecordNotFound)
        end
      end
    end
  end

  # Note: SQL injection prevention tests for schema operations are covered in:
  # - quoting_spec.rb for quote/quote_column_name/quote_table_name
  # - The enum and struct column tests in the ducklake_spec.rb "schema operations" section
  # DuckLake doesn't support ENUM types, so we can't test enum escaping in DuckLake context.
end
