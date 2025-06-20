# frozen_string_literal: true

# Shared examples for DuckDB adapter testing
RSpec.shared_examples 'a working database connection' do
  it 'establishes connection successfully' do
    expect(connection).to be_an_instance_of(ActiveRecord::ConnectionAdapters::DuckdbAdapter)
    expect(connection).to be_active
  end

  it 'executes basic queries' do
    result = connection.execute('SELECT 1 as test')
    expect(result.rows.first.first).to eq(1)
  end

  it 'handles multiple queries' do
    expect { connection.execute('SELECT 1') }.not_to raise_error
    expect { connection.execute('SELECT 2') }.not_to raise_error
    expect { connection.execute('SELECT 3') }.not_to raise_error
  end
end

RSpec.shared_examples 'a Rails-compatible adapter' do
  it 'implements required adapter interface' do
    expect(adapter).to respond_to(:execute)
    expect(adapter).to respond_to(:quote)
    expect(adapter).to respond_to(:quote_column_name)
    expect(adapter).to respond_to(:quote_table_name)
    expect(adapter).to respond_to(:create_table)
    expect(adapter).to respond_to(:drop_table)
    expect(adapter).to respond_to(:columns)
    expect(adapter).to respond_to(:indexes)
  end

  it 'supports ActiveRecord operations' do
    expect(adapter).to respond_to(:supports_insert_returning?)
    expect(adapter).to respond_to(:supports_sequences?)
    expect(adapter).to respond_to(:native_database_types)
  end

  it 'provides database limits' do
    expect(adapter.max_identifier_length).to be_a(Integer)
    expect(adapter.max_identifier_length).to be > 0
    expect(adapter.table_name_length).to eq(adapter.max_identifier_length)
    expect(adapter.index_name_length).to eq(adapter.max_identifier_length)
  end
end

RSpec.shared_examples 'CRUD operations' do |model_setup:|
  let(:model_class) { model_setup.call }

  it 'creates records' do
    record = model_class.create!(name: 'Test Record', value: 42)

    expect(record).to be_persisted
    expect(record.id).to be_present
    expect(record.name).to eq('Test Record')
    expect(record.value).to eq(42)
  end

  it 'reads records' do
    created_record = model_class.create!(name: 'Read Test', value: 100)
    found_record = model_class.find(created_record.id)

    expect(found_record.id).to eq(created_record.id)
    expect(found_record.name).to eq('Read Test')
    expect(found_record.value).to eq(100)
  end

  it 'updates records' do
    record = model_class.create!(name: 'Original', value: 1)
    record.update!(name: 'Updated', value: 2)

    expect(record.reload.name).to eq('Updated')
    expect(record.value).to eq(2)
  end

  it 'deletes records' do
    record = model_class.create!(name: 'To Delete', value: 999)

    expect { record.destroy! }.to change(model_class, :count).by(-1)
    expect { model_class.find(record.id) }.to raise_error(ActiveRecord::RecordNotFound)
  end
end

RSpec.shared_examples 'query operations' do |model_setup:|
  let(:model_class) { model_setup.call }

  before do
    # Create test data
    model_class.create!(name: 'Active Record', value: 100, active: true)
    model_class.create!(name: 'Inactive Record', value: 50, active: false)
    model_class.create!(name: 'Another Active', value: 150, active: true)
  end

  it 'supports where queries' do
    active_records = model_class.where(active: true)
    expect(active_records.count).to eq(2)
  end

  it 'supports order queries' do
    ordered = model_class.order(:value)
    expect(ordered.first.value).to eq(50)
    expect(ordered.last.value).to eq(150)
  end

  it 'supports limit queries' do
    limited = model_class.limit(1)
    expect(limited.count).to eq(1)
  end

  it 'supports aggregations' do
    expect(model_class.count).to eq(3)
    expect(model_class.sum(:value)).to eq(300)
    expect(model_class.average(:value)).to eq(100)
    expect(model_class.maximum(:value)).to eq(150)
    expect(model_class.minimum(:value)).to eq(50)
  end

  it 'supports grouping' do
    grouped = model_class.group(:active).count
    expect(grouped[true]).to eq(2)
    expect(grouped[false]).to eq(1)
  end
end

RSpec.shared_examples 'column type support' do |column_definitions:|
  let(:table_name) { :type_test_table }

  before do
    ActiveRecord::Base.connection.create_table(table_name, &column_definitions)
  end

  after do
    ActiveRecord::Base.connection.drop_table(table_name, if_exists: true)
  end

  it 'creates table with specified columns' do
    expect(ActiveRecord::Base.connection.table_exists?(table_name)).to be true
  end

  it 'defines columns with correct types' do
    columns = ActiveRecord::Base.connection.columns(table_name)
    expect(columns).not_to be_empty

    # Verify basic column properties
    columns.each do |column|
      expect(column.name).to be_a(String)
      expect(column.type).to be_a(Symbol)
      expect(column.sql_type).to be_a(String)
    end
  end
end

RSpec.shared_examples 'standard Rails column types' do
  it_behaves_like 'column type support', column_definitions: proc { |t|
    t.string :string_col, limit: 100
    t.text :text_col
    t.integer :integer_col
    t.bigint :bigint_col
    t.decimal :decimal_col, precision: 10, scale: 2
    t.float :float_col
    t.boolean :boolean_col
    t.date :date_col
    t.datetime :datetime_col
    t.time :time_col
    t.binary :binary_col
    t.uuid :uuid_col
  }

  it 'maps Rails types correctly' do
    columns = ActiveRecord::Base.connection.columns(:type_test_table)

    expect(columns.find { |c| c.name == 'string_col' }.type).to eq(:string)
    expect(columns.find { |c| c.name == 'text_col' }.type).to eq(:string) # DuckDB maps TEXT to string
    expect(columns.find { |c| c.name == 'integer_col' }.type).to eq(:integer)
    expect(columns.find { |c| c.name == 'bigint_col' }.type).to eq(:bigint)
    expect(columns.find { |c| c.name == 'decimal_col' }.type).to eq(:decimal)
    expect(columns.find { |c| c.name == 'float_col' }.type).to eq(:float)
    expect(columns.find { |c| c.name == 'boolean_col' }.type).to eq(:boolean)
    expect(columns.find { |c| c.name == 'date_col' }.type).to eq(:date)
    expect(columns.find { |c| c.name == 'datetime_col' }.type).to eq(:datetime)
    expect(columns.find { |c| c.name == 'time_col' }.type).to eq(:time)
    expect(columns.find { |c| c.name == 'binary_col' }.type).to eq(:binary)
    expect(columns.find { |c| c.name == 'uuid_col' }.type).to eq(:uuid)
  end
end

RSpec.shared_examples 'DuckDB-specific column types' do
  it_behaves_like 'column type support', column_definitions: proc { |t|
    t.hugeint :huge_col
    t.tinyint :tiny_col
    t.utinyint :utiny_col
    t.interval :interval_col
    t.list :list_col, element_type: :string
    t.struct :struct_col, fields: { name: :string, age: :integer }
    t.map :map_col, key_type: :string, value_type: :integer
    t.enum :enum_col, values: %w[active inactive pending]
  }

  it 'creates DuckDB-specific types correctly' do
    columns = ActiveRecord::Base.connection.columns(:type_test_table)

    expect(columns.find { |c| c.name == 'huge_col' }.sql_type).to eq('HUGEINT')
    expect(columns.find { |c| c.name == 'tiny_col' }.sql_type).to eq('TINYINT')
    expect(columns.find { |c| c.name == 'utiny_col' }.sql_type).to eq('UTINYINT')
    expect(columns.find { |c| c.name == 'interval_col' }.sql_type).to eq('INTERVAL')
    expect(columns.find { |c| c.name == 'list_col' }.sql_type).to eq('VARCHAR[]')
    expect(columns.find { |c| c.name == 'struct_col' }.sql_type).to eq('STRUCT(name VARCHAR, age INTEGER)')
    expect(columns.find { |c| c.name == 'map_col' }.sql_type).to eq('MAP(VARCHAR, INTEGER)')
    expect(columns.find { |c| c.name == 'enum_col' }.sql_type).to eq("ENUM('active', 'inactive', 'pending')")
  end
end

RSpec.shared_examples 'primary key variants' do
  context 'with integer primary key' do
    it 'creates table with bigint id by default' do
      ActiveRecord::Base.connection.create_table(:int_pk_test) { |t| t.string :name }

      id_column = ActiveRecord::Base.connection.columns(:int_pk_test).find { |c| c.name == 'id' }
      expect(id_column.type).to eq(:bigint)
      expect(id_column.sql_type).to eq('BIGINT')
    end
  end

  context 'with UUID primary key' do
    it 'creates table with UUID id' do
      ActiveRecord::Base.connection.create_table(:uuid_pk_test, id: :uuid) { |t| t.string :name }

      id_column = ActiveRecord::Base.connection.columns(:uuid_pk_test).find { |c| c.name == 'id' }
      expect(id_column.type).to eq(:uuid)
      expect(id_column.sql_type).to eq('UUID')
    end
  end

  context 'with string primary key' do
    it 'creates table with string id' do
      ActiveRecord::Base.connection.create_table(:string_pk_test, id: :string) { |t| t.string :name }

      id_column = ActiveRecord::Base.connection.columns(:string_pk_test).find { |c| c.name == 'id' }
      expect(id_column.type).to eq(:string)
      expect(id_column.sql_type).to match(/VARCHAR/i)
    end
  end

  context 'without primary key' do
    it 'creates table without id column' do
      ActiveRecord::Base.connection.create_table(:no_pk_test, id: false) { |t| t.string :name }

      columns = ActiveRecord::Base.connection.columns(:no_pk_test)
      id_column = columns.find { |c| c.name == 'id' }
      expect(id_column).to be_nil
    end
  end

  after do
    %i[int_pk_test uuid_pk_test string_pk_test no_pk_test].each do |table|
      ActiveRecord::Base.connection.drop_table(table, if_exists: true)
    end
  end
end

RSpec.shared_examples 'sequence support' do
  let(:sequence_name) { 'test_sequence' }

  it 'creates and manages sequences' do
    adapter.create_sequence(sequence_name, start_with: 100)
    expect(adapter.sequence_exists?(sequence_name)).to be true

    adapter.reset_sequence!(sequence_name, 200)
    adapter.drop_sequence(sequence_name)
    expect(adapter.sequence_exists?(sequence_name)).to be false
  end

  it 'handles sequence errors gracefully' do
    # Creating duplicate sequence should not crash
    adapter.create_sequence(sequence_name)
    expect { adapter.create_sequence(sequence_name) }.not_to raise_error

    adapter.drop_sequence(sequence_name)
  end

  after do
    adapter.drop_sequence(sequence_name, if_exists: true)
  end
end

RSpec.shared_examples 'index operations' do
  let(:table_name) { :index_test_table }

  before do
    ActiveRecord::Base.connection.create_table(table_name) do |t|
      t.string :email
      t.string :username
      t.string :category
      t.integer :score
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(table_name, if_exists: true)
  end

  it 'creates single column indexes' do
    adapter.add_index(table_name, :email, unique: true, name: 'idx_email')

    indexes = adapter.indexes(table_name)
    email_index = indexes.find { |idx| idx.name == 'idx_email' }

    expect(email_index).not_to be_nil
    expect(email_index.unique).to be true
    expect(email_index.columns).to eq(['email'])
  end

  it 'creates composite indexes' do
    adapter.add_index(table_name, %i[category score], name: 'idx_category_score')
    indexes = adapter.indexes(table_name)
    composite_index = indexes.find { |idx| idx.name == 'idx_category_score' }

    expect(composite_index).not_to be_nil
    expect(composite_index.columns).to eq(%w[category score])
  end

  it 'removes indexes' do
    adapter.add_index(table_name, :username, name: 'idx_username')
    expect(adapter.indexes(table_name).map(&:name)).to include('idx_username')

    adapter.remove_index(table_name, name: 'idx_username')
    expect(adapter.indexes(table_name).map(&:name)).not_to include('idx_username')
  end
end

RSpec.shared_examples 'database tasks' do |config_hash:|
  let(:db_config) { instance_double(DatabaseConfig, configuration_hash: config_hash) }
  let(:tasks) { ActiveRecord::Tasks::DuckdbDatabaseTasks.new(db_config) }

  it 'supports configuration queries' do
    expect(ActiveRecord::Tasks::DuckdbDatabaseTasks.using_database_configurations?).to be true
  end

  it 'provides database metadata' do
    expect(tasks.charset).to eq('UTF-8')
    expect(tasks.collation).to be_nil
  end

  it 'handles database lifecycle' do
    expect { tasks.create }.not_to raise_error
    expect { tasks.drop }.not_to raise_error
    expect { tasks.purge }.not_to raise_error
  end

  it 'supports structure operations with temp files' do
    require 'tempfile'

    Tempfile.create(['structure', '.sql']) do |temp_file|
      expect { tasks.structure_dump(temp_file.path) }.not_to raise_error
      expect { tasks.structure_load(temp_file.path) }.not_to raise_error
    end
  end
end

RSpec.shared_examples 'error handling' do
  it 'handles SQL syntax errors' do
    expect { adapter.execute('INVALID SQL STATEMENT') }.to raise_error(DuckDB::Error)
  end

  it 'handles missing table errors' do
    expect { adapter.columns('nonexistent_table') }.to raise_error
  end

  it 'handles invalid column references' do
    adapter.create_table(:error_test) { |t| t.string :name }
    expect { adapter.execute('SELECT nonexistent_column FROM error_test') }.to raise_error(DuckDB::Error)

    adapter.drop_table(:error_test)
  end

  it 'handles constraint violations gracefully' do
    adapter.create_table(:constraint_test) do |t|
      t.string :email, null: false
    end

    # NULL constraint violation
    expect { adapter.execute('INSERT INTO constraint_test (email) VALUES (NULL)') }.to raise_error(DuckDB::Error)

    adapter.drop_table(:constraint_test)
  end
end

RSpec.shared_examples 'migration compatibility' do
  it 'supports Rails migration syntax' do
    migration_class = Class.new(ActiveRecord::Migration[7.0]) do
      def up
        create_table :migration_compatibility_test do |t|
          t.string :name
          t.integer :age
          t.timestamps
        end

        add_index :migration_compatibility_test, :name
      end

      def down
        drop_table :migration_compatibility_test
      end
    end

    migration = migration_class.new

    expect { migration.up }.not_to raise_error
    expect(adapter.table_exists?(:migration_compatibility_test)).to be true

    indexes = adapter.indexes(:migration_compatibility_test)
    expect(indexes.any? { |idx| idx.columns.include?('name') }).to be true

    expect { migration.down }.not_to raise_error
    expect(adapter.table_exists?(:migration_compatibility_test)).to be false
  end
end

RSpec.shared_context 'with established connection' do |config = nil|
  let(:connection_config) { config || memory_config }
  let(:adapter) { establish_test_connection(connection_config) }

  after { remove_test_connection }
end
