# frozen_string_literal: true

module TestHelpers
  # Database connection helpers
  def memory_config
    {
      adapter: 'duckdb',
      database: ':memory:'
    }
  end

  def file_config(database_path = 'tmp/test.duckdb')
    {
      adapter: 'duckdb',
      database: database_path
    }
  end

  def establish_test_connection(config = memory_config)
    ActiveRecord::Base.establish_connection(config)
    ActiveRecord::Base.connection
  end

  def remove_test_connection
    ActiveRecord::Base.remove_connection
  end

  # Model creation helpers
  def create_test_model(table_name, &)
    model_class = Class.new(ActiveRecord::Base) do
      self.table_name = table_name.to_s
    end

    model_class.class_eval(&) if block_given?
    model_class
  end

  def with_test_table(table_name, &)
    connection = ActiveRecord::Base.connection
    connection.create_table(table_name, &)

    yield if block_given?
  ensure
    connection&.drop_table(table_name, if_exists: true)
  end

  def with_test_model(table_name, table_definition: nil, model_definition: nil)
    connection = ActiveRecord::Base.connection

    # Create table with provided definition or empty default
    if table_definition
      connection.create_table(table_name, &table_definition)
    else
      connection.create_table(table_name)
    end

    model = create_test_model(table_name)
    model.class_eval(&model_definition) if model_definition
    yield model
  ensure
    connection&.drop_table(table_name, if_exists: true)
  end

  # Assertion helpers
  def expect_column_type(table_name, column_name, expected_type)
    columns = ActiveRecord::Base.connection.columns(table_name)
    column = columns.find { |c| c.name == column_name.to_s }

    expect(column).not_to be_nil
    expect(column.type).to eq(expected_type)
  end

  def expect_column_sql_type(table_name, column_name, expected_sql_type)
    columns = ActiveRecord::Base.connection.columns(table_name)
    column = columns.find { |c| c.name == column_name.to_s }

    expect(column).not_to be_nil
    expect(column.sql_type.upcase).to eq(expected_sql_type.upcase)
  end

  def expect_table_exists(table_name)
    expect(ActiveRecord::Base.connection.table_exists?(table_name)).to be true
  end

  def expect_table_not_exists(table_name)
    expect(ActiveRecord::Base.connection.table_exists?(table_name)).to be false
  end

  # Query helpers
  def execute_sql(sql)
    ActiveRecord::Base.connection.execute(sql)
  end

  def query_value(sql)
    result = execute_sql(sql)
    result.rows.first&.first
  end

  def count_records(table_name, conditions = nil)
    sql = "SELECT COUNT(*) FROM #{table_name}"
    sql += " WHERE #{conditions}" if conditions
    query_value(sql)
  end

  # Test data creation helpers
  def create_sample_data(model_class, count = 5)
    count.times do |i|
      attributes = {
        name: "Sample #{i}",
        value: i * 10,
        active: i.even?
      }

      # Only include attributes that the model actually has
      valid_attributes = attributes.select do |key, _|
        model_class.column_names.include?(key.to_s)
      end

      model_class.create!(valid_attributes)
    end
  end

  def generate_test_email(prefix = 'test')
    "#{prefix}#{SecureRandom.hex(4)}@example.com"
  end

  def generate_test_uuid
    SecureRandom.uuid
  end

  def create_bulk_data(model_class, count = 1000, batch_size = 100)
    (0...count).each_slice(batch_size) do |batch|
      records = batch.map do |i|
        {
          name: "Bulk #{i}",
          value: i,
          active: i.even?,
          created_at: Time.current,
          updated_at: Time.current
        }.select { |key, _| model_class.column_names.include?(key.to_s) }
      end
      model_class.insert_all(records)
    end
  end

  # Sequence helpers
  def create_test_sequence(sequence_name, start_with: 1)
    ActiveRecord::Base.connection.create_sequence(sequence_name, start_with: start_with)
  end

  def drop_test_sequence(sequence_name)
    ActiveRecord::Base.connection.drop_sequence(sequence_name, if_exists: true)
  end

  def sequence_exists?(sequence_name)
    ActiveRecord::Base.connection.sequence_exists?(sequence_name)
  end

  # Type conversion helpers
  def duckdb_type_for(rails_type, **)
    adapter = ActiveRecord::Base.connection
    adapter.type_to_sql(rails_type, **)
  end

  # Error handling helpers
  def expect_duckdb_error(&)
    expect(&).to raise_error(DuckDB::Error)
  end

  def expect_activerecord_error(error_class = ActiveRecord::StatementInvalid, &)
    expect(&).to raise_error(error_class)
  end

  # File system helpers
  def with_temp_directory(&)
    require 'tmpdir'
    Dir.mktmpdir(&)
  end

  def with_temp_file(extension = '.sql', &)
    require 'tempfile'
    Tempfile.create(['test', extension], &)
  end

  # Performance helpers
  def measure_time(&)
    start_time = Time.current
    result = yield
    end_time = Time.current
    [result, end_time - start_time]
  end

  def expect_fast_execution(max_time = 1.0, &)
    result, execution_time = measure_time(&)
    expect(execution_time).to be < max_time
    result
  end

  def benchmark_query(sql, iterations = 100)
    times = []
    iterations.times do
      _, time = measure_time { execute_sql(sql) }
      times << time
    end
    {
      min: times.min,
      max: times.max,
      avg: times.sum / times.size,
      median: times.sort[times.size / 2]
    }
  end

  # Database state helpers
  def clean_database
    connection = ActiveRecord::Base.connection

    # Get all tables except system tables
    tables = connection.tables.reject do |table|
      %w[ar_internal_metadata schema_migrations sqlite_sequence].include?(table)
    end

    # Drop all tables
    tables.each do |table|
      connection.drop_table(table, if_exists: true)
    end

    # Clean up sequences
    begin
      sequences = connection.sequences
      sequences.each { |seq| connection.drop_sequence(seq, if_exists: true) }
    rescue NotImplementedError
      # Some adapters might not support sequence listing
    end
  end

  # Complex data type helpers
  def create_list_column(table_name, column_name, element_type: :string)
    connection = ActiveRecord::Base.connection
    connection.change_table(table_name) do |t|
      t.list column_name, element_type: element_type
    end
  end

  def create_struct_column(table_name, column_name, fields:)
    connection = ActiveRecord::Base.connection
    connection.change_table(table_name) do |t|
      t.struct column_name, fields: fields
    end
  end

  def create_map_column(table_name, column_name, key_type: :string, value_type: :string)
    connection = ActiveRecord::Base.connection
    connection.change_table(table_name) do |t|
      t.map column_name, key_type: key_type, value_type: value_type
    end
  end

  def create_enum_column(table_name, column_name, values:)
    connection = ActiveRecord::Base.connection
    connection.change_table(table_name) do |t|
      t.enum column_name, values: values
    end
  end

  # Debugging helpers
  def debug_table_structure(table_name)
    connection = ActiveRecord::Base.connection
    columns = connection.columns(table_name)

    puts "\n=== Table: #{table_name} ==="
    columns.each do |column|
      puts "#{column.name}: #{column.sql_type} (#{column.type}) - null: #{column.null}, default: #{column.default}"
    end
    puts "========================\n"
  end

  def debug_query_result(sql)
    result = execute_sql(sql)
    puts "\n=== Query: #{sql} ==="
    puts "Columns: #{result.columns.join(", ")}"
    result.rows.each_with_index do |row, index|
      puts "Row #{index}: #{row.join(", ")}"
    end
    puts "========================\n"
  end

  def debug_indexes(table_name)
    indexes = ActiveRecord::Base.connection.indexes(table_name)
    puts "\n=== Indexes for #{table_name} ==="
    indexes.each do |index|
      puts "#{index.name}: #{index.columns.join(", ")} (unique: #{index.unique})"
    end
    puts "========================\n"
  end

  def debug_sequences
    adapter = ActiveRecord::Base.connection
    return unless adapter.respond_to?(:sequences)

    sequences = adapter.sequences
    puts "\n=== Sequences ==="
    sequences.each { |seq| puts seq }
    puts "========================\n"
  end

  # Rails environment helpers
  def with_rails_env(env = 'test')
    original_env = ENV.fetch('RAILS_ENV', nil)
    ENV['RAILS_ENV'] = env
    yield
  ensure
    ENV['RAILS_ENV'] = original_env
  end

  def stub_rails_logger
    return unless defined?(Rails)

    allow(Rails).to receive(:logger).and_return(Logger.new(File::NULL))
  end

  # Transaction helpers
  def in_transaction(&)
    ActiveRecord::Base.transaction(&)
  end

  def with_rollback(&)
    ActiveRecord::Base.transaction do
      yield
      raise ActiveRecord::Rollback
    end
  end

  def expect_rollback(&)
    expect { with_rollback(&) }.not_to raise_error
  end

  # Configuration helpers
  def with_primary_key_type(type)
    adapter_class = ActiveRecord::ConnectionAdapters::DuckdbAdapter
    original_type = adapter_class.primary_key_type
    adapter_class.primary_key_type = type
    yield
  ensure
    adapter_class.primary_key_type = original_type
  end

  # Migration helpers
  def create_test_migration(&)
    Class.new(ActiveRecord::Migration[7.0]) do
      define_method(:up, &)

      def down
        # Default down method - can be overridden
      end
    end
  end

  def run_migration_up(migration_class)
    migration = migration_class.new
    migration.up
  end

  def run_migration_down(migration_class)
    migration = migration_class.new
    migration.down
  end

  def create_reversible_migration(up_proc, down_proc)
    Class.new(ActiveRecord::Migration[7.0]) do
      define_method(:up, &up_proc)
      define_method(:down, &down_proc)
    end
  end

  # Concurrency helpers
  def run_concurrent_operations(thread_count = 5, &)
    threads = []
    results = []
    mutex = Mutex.new

    thread_count.times do |i|
      threads << Thread.new do
        result = yield(i)
        mutex.synchronize { results << { thread: i, result: result, error: nil } }
      rescue StandardError => e
        mutex.synchronize { results << { thread: i, result: nil, error: e } }
      end
    end

    threads.each(&:join)
    results.sort_by { |r| r[:thread] }
  end

  def expect_thread_safe_operation(thread_count = 5, &)
    results = run_concurrent_operations(thread_count, &)
    errors = results.select { |r| r[:error] }

    expect(errors).to be_empty, "Expected no errors, but got: #{errors.map { |e| e[:error].message }}"
    results.map { |r| r[:result] }
  end
end

# Schema comparison helpers
def compare_table_schemas(table1, table2)
  columns1 = ActiveRecord::Base.connection.columns(table1).sort_by(&:name)
  columns2 = ActiveRecord::Base.connection.columns(table2).sort_by(&:name)

  differences = []

  columns1.each do |col1|
    col2 = columns2.find { |c| c.name == col1.name }
    if col2.nil?
      differences << "Column #{col1.name} exists in #{table1} but not in #{table2}"
    elsif col1.type != col2.type
      differences << "Column #{col1.name} type differs: #{col1.type} vs #{col2.type}"
    elsif col1.null != col2.null
      differences << "Column #{col1.name} null constraint differs: #{col1.null} vs #{col2.null}"
    end
  end

  columns2.each do |col2|
    col1 = columns1.find { |c| c.name == col2.name }
    differences << "Column #{col2.name} exists in #{table2} but not in #{table1}" if col1.nil?
  end

  differences
end

def expect_matching_schemas(table1, table2)
  differences = compare_table_schemas(table1, table2)
  expect(differences).to be_empty, "Schema differences found: #{differences.join(", ")}"
end

# Validation helpers
def expect_valid_record(record)
  expect(record).to be_valid, "Expected record to be valid, errors: #{record.errors.full_messages}"
end

def expect_invalid_record(record, expected_errors = {})
  expect(record).not_to be_valid
  expected_errors.each do |field, message|
    expect(record.errors[field]).to include(message)
  end
end

# Database state helpers
def wait_for_connection(timeout = 5)
  start_time = Time.current
  loop do
    break if ActiveRecord::Base.connected?
    break if Time.current - start_time > timeout

    sleep 0.1
  end
  ActiveRecord::Base.connected?
end

def count_all_records
  ActiveRecord::Base.connection.tables.sum do |table|
    count_records(table)
  rescue StandardError
    0
  end
end

# Include helpers in RSpec
RSpec.configure do |config|
  config.include TestHelpers

  # Set up clean database state for each test
  config.before do
    clean_database if defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?
  end

  # Clean up connections after tests
  config.after do
    ActiveRecord::Base.remove_connection if defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?
  end

  # Global setup
  config.before(:suite) do
    # Ensure the adapter is loaded
    require 'activerecord-duckdb'
  end
end
