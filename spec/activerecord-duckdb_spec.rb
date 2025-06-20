# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'securerandom'

RSpec.describe 'ActiveRecord::DuckDB Integration' do
  describe 'adapter registration and loading' do
    it 'loads the gem without errors' do
      expect { require 'activerecord-duckdb' }.not_to raise_error
    end

    it 'makes the adapter class available' do
      expect(ActiveRecord::ConnectionAdapters::DuckdbAdapter).to be < ActiveRecord::ConnectionAdapters::AbstractAdapter
    end

    # it 'defines the version constant' do
    #   expect(Activerecord::Duckdb::VERSION).to match(/\A\d+\.\d+\.\d+\z/)
    # end

    it 'registers the adapter for connections' do
      config = { adapter: 'duckdb', database: ':memory:' }
      expect { ActiveRecord::Base.duckdb_connection(config) }.not_to raise_error
    end
  end

  describe 'database connections' do
    after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

    context 'with memory database' do
      let(:config) { { adapter: 'duckdb', database: ':memory:' } }

      it 'establishes a working connection' do
        ActiveRecord::Base.establish_connection(config)

        expect(ActiveRecord::Base.connection).to be_an_instance_of(ActiveRecord::ConnectionAdapters::DuckdbAdapter)
        expect { ActiveRecord::Base.connection.execute('SELECT 1') }.not_to raise_error
      end

      it 'executes basic SQL queries' do
        ActiveRecord::Base.establish_connection(config)

        result = ActiveRecord::Base.connection.execute('SELECT 1 as test_value')
        expect(result.to_a.first.first).to eq(1)
      end
    end

    context 'with file database' do
      let(:temp_db_path) { File.join(Dir.tmpdir, "test_#{SecureRandom.hex(8)}.duckdb") }
      let(:config) { { adapter: 'duckdb', database: temp_db_path } }

      after { FileUtils.rm_f(temp_db_path) }

      it 'creates and connects to a file database' do
        ActiveRecord::Base.establish_connection(config)
        ActiveRecord::Base.connection.execute('SELECT 1') # Trigger database creation

        expect(File).to exist(temp_db_path)
      end
    end
  end

  describe 'ActiveRecord model operations' do
    before do
      ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')

      # Ensure clean slate - drop table if it exists
      begin
        ActiveRecord::Base.connection.drop_table(:test_users)
      rescue ActiveRecord::StatementInvalid, DuckDB::Error
        # Ignore if table doesn't exist
      end

      # Create table using Rails migration syntax
      ActiveRecord::Base.connection.create_table(:test_users, force: true) do |t|
        t.string :name, null: false
        t.integer :age
        t.boolean :active, default: true
        t.timestamps
      end
    end

    after do
      # Clean up table and connection
      begin
        ActiveRecord::Base.connection.drop_table(:test_users) if ActiveRecord::Base.connection.table_exists?(:test_users)
      rescue ActiveRecord::StatementInvalid, DuckDB::Error
        # Ignore cleanup errors
      end
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
    end

    let(:user_class) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'test_users'
        validates :name, presence: true

        def self.name
          'TestUser'
        end
      end
    end

    describe 'CRUD operations' do
      it 'supports creating records' do
        user = user_class.create!(name: 'John Doe', age: 30)

        expect(user).to be_persisted
        expect(user.id).to be_present
        expect(user.name).to eq('John Doe')
        expect(user.age).to eq(30)
        expect(user.active).to be true
      end

      it 'supports reading records' do
        created_user = user_class.create!(name: 'Jane Doe', age: 25)
        found_user = user_class.find(created_user.id)

        expect(found_user.name).to eq('Jane Doe')
        expect(found_user.age).to eq(25)
      end

      it 'supports updating records' do
        user = user_class.create!(name: 'Original Name', age: 20)

        user.update!(name: 'Updated Name', age: 21)

        expect(user.reload.name).to eq('Updated Name')
        expect(user.age).to eq(21)
      end

      it 'supports deleting records' do
        user = user_class.create!(name: 'To Be Deleted')

        expect { user.destroy! }.to change(user_class, :count).by(-1)
      end

      it 'validates model constraints' do
        invalid_user = user_class.new # Missing required name

        expect(invalid_user).not_to be_valid
        expect(invalid_user.errors[:name]).to include("can't be blank")
      end
    end

    describe 'query operations' do
      before do
        # Clear any existing data
        user_class.delete_all

        # Create test data
        user_class.create!(name: 'Active User', age: 25, active: true)
        user_class.create!(name: 'Inactive User', age: 30, active: false)
        user_class.create!(name: 'Another Active', age: 35, active: true)
      end

      it 'supports where clauses' do
        active_users = user_class.where(active: true)
        expect(active_users.count).to eq(2)
      end

      it 'supports ordering' do
        ordered_users = user_class.order(:age)
        expect(ordered_users.first.age).to eq(25)
        expect(ordered_users.last.age).to eq(35)
      end

      it 'supports aggregations' do
        expect(user_class.count).to eq(3)
        expect(user_class.sum(:age)).to eq(90)
        expect(user_class.average(:age)).to eq(30)
        expect(user_class.maximum(:age)).to eq(35)
        expect(user_class.minimum(:age)).to eq(25)
      end

      it 'supports grouping' do
        grouped = user_class.group(:active).count
        expect(grouped[true]).to eq(2)
        expect(grouped[false]).to eq(1)
      end
    end
  end

  describe 'schema operations' do
    before { ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:') }
    after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

    it 'supports standard Rails column types' do
      ActiveRecord::Base.connection.create_table(:type_showcase) do |t|
        t.string :string_col, limit: 100
        t.text :text_col
        t.integer :integer_col
        t.bigint :bigint_col
        t.decimal :decimal_col, precision: 10, scale: 2
        t.float :float_col
        t.date :date_col
        t.datetime :datetime_col
        t.uuid :uuid_col
        t.binary :binary_col
      end

      columns = ActiveRecord::Base.connection.columns(:type_showcase)

      expect(columns.find { |c| c.name == 'string_col' }.type).to eq(:string)
      expect(columns.find { |c| c.name == 'integer_col' }.type).to eq(:integer)
      expect(columns.find { |c| c.name == 'bigint_col' }.type).to eq(:bigint)
      expect(columns.find { |c| c.name == 'decimal_col' }.type).to eq(:decimal)
      expect(columns.find { |c| c.name == 'uuid_col' }.type).to eq(:uuid)
    end

    it 'supports different primary key types' do
      # Default bigint primary key
      ActiveRecord::Base.connection.create_table(:default_pk) do |t|
        t.string :name
      end
      default_id = ActiveRecord::Base.connection.columns(:default_pk).find { |c| c.name == 'id' }
      expect(default_id.type).to eq(:bigint)

      # UUID primary key
      ActiveRecord::Base.connection.create_table(:uuid_pk, id: :uuid) do |t|
        t.string :name
      end
      uuid_id = ActiveRecord::Base.connection.columns(:uuid_pk).find { |c| c.name == 'id' }
      expect(uuid_id.type).to eq(:uuid)
    end

    it 'supports DuckDB-specific column types' do
      ActiveRecord::Base.connection.create_table(:duckdb_features) do |t|
        t.hugeint :huge_number
        t.tinyint :tiny_number
        t.interval :duration
        t.list :tags, element_type: :string
        t.struct :contact, fields: { email: :string, phone: :string }
        t.map :metadata, key_type: :string, value_type: :string
        t.enum :status, values: %w[active inactive]
      end

      columns = ActiveRecord::Base.connection.columns(:duckdb_features)

      expect(columns.find { |c| c.name == 'huge_number' }.sql_type).to eq('HUGEINT')
      expect(columns.find { |c| c.name == 'tiny_number' }.sql_type).to eq('TINYINT')
      expect(columns.find { |c| c.name == 'duration' }.sql_type).to eq('INTERVAL')
      expect(columns.find { |c| c.name == 'tags' }.sql_type).to eq('VARCHAR[]')
      expect(columns.find { |c| c.name == 'status' }.sql_type).to eq("ENUM('active', 'inactive')")
    end
  end

  describe 'adapter features' do
    before { ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:') }
    after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

    let(:adapter) { ActiveRecord::Base.connection }

    it 'supports sequences' do
      expect(adapter.send(:supports_sequences?)).to be true

      sequence_name = 'test_sequence'
      adapter.create_sequence(sequence_name, start_with: 100)
      expect(adapter.sequence_exists?(sequence_name)).to be true

      adapter.drop_sequence(sequence_name)
      expect(adapter.sequence_exists?(sequence_name)).to be false
    end

    it 'supports insert returning' do
      expect(adapter.supports_insert_returning?).to be true
      expect(adapter.use_insert_returning?).to be true
    end

    it 'provides proper database limits' do
      expect(adapter.max_identifier_length).to eq(63)
      expect(adapter.table_name_length).to eq(63)
      expect(adapter.index_name_length).to eq(63)
    end

    it 'handles indexes' do
      ActiveRecord::Base.connection.create_table(:indexed_example) do |t|
        t.string :email
        t.string :username
        t.string :category
      end

      adapter.add_index(:indexed_example, :email, unique: true, name: 'idx_email')
      adapter.add_index(:indexed_example, %i[username category], name: 'idx_user_cat')

      indexes = adapter.indexes(:indexed_example)

      expect(indexes.map(&:name)).to include('idx_email', 'idx_user_cat')

      email_index = indexes.find { |idx| idx.name == 'idx_email' }
      expect(email_index.unique).to be true
      expect(email_index.columns).to eq(['email'])
    end
  end

  describe 'database tasks' do
    it 'provides database configuration support' do
      expect(ActiveRecord::Tasks::DuckdbDatabaseTasks.using_database_configurations?).to be true
    end

    it 'handles basic database operations' do
      config_hash = { adapter: 'duckdb', database: ':memory:' }
      db_config = double('DatabaseConfig', configuration_hash: config_hash)
      tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(db_config, Dir.tmpdir)

      expect(tasks.charset).to eq('UTF-8')
      expect(tasks.collation).to be_nil

      expect { tasks.create }.not_to raise_error
      expect { tasks.drop }.not_to raise_error
      expect { tasks.purge }.not_to raise_error
    end
  end

  describe 'error handling' do
    before { ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:') }
    after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

    it 'handles SQL syntax errors gracefully' do
      expect do
        ActiveRecord::Base.connection.execute('INVALID SQL STATEMENT')
      end.to raise_error(DuckDB::Error)
    end

    it 'handles missing table errors' do
      expect do
        ActiveRecord::Base.connection.columns('nonexistent_table')
      end.to raise_error(DuckDB::Error)
    end
  end

  describe 'version compatibility' do
    it 'requires supported ActiveRecord version' do
      ar_version = Gem::Version.new(ActiveRecord::VERSION::STRING)
      minimum_version = Gem::Version.new('7.0.0')
      expect(ar_version).to be >= minimum_version
    end

    it 'works with current Ruby version' do
      ruby_version = Gem::Version.new(RUBY_VERSION)
      minimum_ruby = Gem::Version.new('3.1.0')
      expect(ruby_version).to be >= minimum_ruby
    end
  end
end
