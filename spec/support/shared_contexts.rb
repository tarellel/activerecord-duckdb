# frozen_string_literal: true

# Shared contexts for DuckDB adapter testing

RSpec.shared_context 'with memory database' do
  let(:config) { memory_config }
  let(:connection) { establish_test_connection(config) }

  after { remove_test_connection }
end

RSpec.shared_context 'with file database' do
  let(:temp_db_path) { File.join(Dir.tmpdir, "test_#{SecureRandom.hex(8)}.duckdb") }
  let(:config) { file_config(temp_db_path) }
  let(:connection) { establish_test_connection(config) }

  after do
    remove_test_connection
    FileUtils.rm_f(temp_db_path)
  end
end

RSpec.shared_context 'with adapter' do
  include_context 'with memory database'
  let(:adapter) { connection }
end

RSpec.shared_context 'with basic test table' do
  let(:table_name) { :test_records }

  before do
    ActiveRecord::Base.connection.create_table(table_name) do |t|
      t.string :name
      t.integer :value
      t.boolean :active, default: true
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(table_name, if_exists: true)
  end
end

RSpec.shared_context 'with test model' do
  include_context 'with basic test table'

  let(:model_class) do
    Class.new(ActiveRecord::Base) do
      self.table_name = :test_records
      validates :name, presence: true
      scope :active, -> { where(active: true) }
      scope :by_value, ->(val) { where(value: val) }
    end
  end
end

RSpec.shared_context 'with sample data' do
  before do
    model_class.create!(name: 'First Record', value: 10, active: true)
    model_class.create!(name: 'Second Record', value: 20, active: false)
    model_class.create!(name: 'Third Record', value: 30, active: true)
    model_class.create!(name: 'Fourth Record', value: 40, active: true)
    model_class.create!(name: 'Fifth Record', value: 50, active: false)
  end
end

RSpec.shared_context 'with users and posts tables' do
  let(:users_table) { :users }
  let(:posts_table) { :posts }

  before do
    ActiveRecord::Base.connection.create_table(users_table) do |t|
      t.string :name, null: false
      t.string :email, null: false
      t.boolean :active, default: true
      t.timestamps
    end

    ActiveRecord::Base.connection.create_table(posts_table) do |t|
      t.string :title, null: false
      t.text :content
      t.references :user, null: false, foreign_key: false
      t.boolean :published, default: false
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(posts_table, if_exists: true)
    ActiveRecord::Base.connection.drop_table(users_table, if_exists: true)
  end
end

RSpec.shared_context 'with user and post models' do
  include_context 'with users and posts tables'

  let(:user_class) do
    Class.new(ActiveRecord::Base) do
      self.table_name = :users
      validates :name, :email, presence: true
      validates :email, uniqueness: true
      scope :active, -> { where(active: true) }
    end
  end

  let(:post_class) do
    Class.new(ActiveRecord::Base) do
      self.table_name = :posts
      validates :title, presence: true
      scope :published, -> { where(published: true) }
      scope :by_user, ->(user_id) { where(user_id: user_id) }
    end
  end
end

RSpec.shared_context 'with sample users and posts' do
  let(:user1) { user_class.create!(name: 'Alice', email: 'alice@example.com', active: true) }
  let(:user2) { user_class.create!(name: 'Bob', email: 'bob@example.com', active: false) }
  let(:user3) { user_class.create!(name: 'Charlie', email: 'charlie@example.com', active: true) }

  let(:post1) { post_class.create!(title: 'First Post', content: 'Hello World', user_id: user1.id, published: true) }
  let(:post2) do
    post_class.create!(title: 'Second Post', content: 'More content', user_id: user1.id, published: false)
  end
  let(:post3) { post_class.create!(title: 'Third Post', content: 'Even more', user_id: user2.id, published: true) }
  let(:post4) { post_class.create!(title: 'Fourth Post', content: 'Last one', user_id: user3.id, published: true) }
end

RSpec.shared_context 'with type testing table' do
  let(:type_table) { :type_test }

  before do
    ActiveRecord::Base.connection.create_table(type_table) do |t|
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
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(type_table, if_exists: true)
  end
end

RSpec.shared_context 'with duckdb specific types table' do
  let(:duckdb_table) { :duckdb_types }

  before do
    ActiveRecord::Base.connection.create_table(duckdb_table) do |t|
      t.hugeint :huge_number
      t.tinyint :tiny_number
      t.utinyint :unsigned_tiny
      t.interval :duration
      t.list :tags, element_type: :string
      t.struct :address, fields: { street: :string, city: :string, zip: :integer }
      t.map :metadata, key_type: :string, value_type: :string
      t.enum :status, values: %w[active inactive pending archived]
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(duckdb_table, if_exists: true)
  end
end

RSpec.shared_context 'with primary key variants' do
  let(:uuid_table) { :uuid_pk_test }
  let(:string_table) { :string_pk_test }
  let(:integer_table) { :integer_pk_test }
  let(:no_pk_table) { :no_pk_test }

  before do
    # UUID primary key
    ActiveRecord::Base.connection.create_table(uuid_table, id: :uuid) do |t|
      t.string :name
      t.timestamps
    end

    # String primary key
    ActiveRecord::Base.connection.create_table(string_table, id: :string) do |t|
      t.string :name
      t.timestamps
    end

    # Integer primary key (explicit)
    ActiveRecord::Base.connection.create_table(integer_table, id: :integer) do |t|
      t.string :name
      t.timestamps
    end

    # No primary key
    ActiveRecord::Base.connection.create_table(no_pk_table, id: false) do |t|
      t.string :name
      t.string :code
      t.timestamps
    end
  end

  after do
    [uuid_table, string_table, integer_table, no_pk_table].each do |table|
      ActiveRecord::Base.connection.drop_table(table, if_exists: true)
    end
  end
end

RSpec.shared_context 'with indexable table' do
  let(:indexed_table) { :indexed_test }

  before do
    ActiveRecord::Base.connection.create_table(indexed_table) do |t|
      t.string :email
      t.string :username
      t.string :first_name
      t.string :last_name
      t.integer :age
      t.string :category
      t.decimal :score, precision: 10, scale: 2
      t.boolean :active, default: true
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(indexed_table, if_exists: true)
  end
end

RSpec.shared_context 'with sequence testing' do
  let(:sequence_names) { %w[test_seq_1 test_seq_2 custom_sequence] }

  after do
    sequence_names.each do |seq_name|
      ActiveRecord::Base.connection.drop_sequence(seq_name, if_exists: true)
    end
  end
end

RSpec.shared_context 'with migration testing' do
  let(:migration_table) { :migration_test }

  let(:test_migration) do
    Class.new(ActiveRecord::Migration[7.0]) do
      def up
        create_table :migration_test do |t|
          t.string :name
          t.integer :version, default: 1
          t.timestamps
        end

        add_index :migration_test, :name
        add_index :migration_test, %i[name version], unique: true
      end

      def down
        drop_table :migration_test
      end
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(migration_table, if_exists: true)
  end
end

RSpec.shared_context 'with database tasks' do
  let(:memory_config_hash) { memory_config }
  let(:file_config_hash) { file_config('tmp/test_tasks.duckdb') }
  let(:memory_db_config) { instance_double(MemoryConfig, configuration_hash: memory_config_hash) }
  let(:file_db_config) { instance_double(FileConfig, configuration_hash: file_config_hash) }

  after do
    # Clean up any test database files
    test_db_path = file_config_hash[:database]
    FileUtils.rm_f(test_db_path)
  end
end

RSpec.shared_context 'with error scenarios' do
  let(:error_table) { :error_test }

  before do
    ActiveRecord::Base.connection.create_table(error_table) do |t|
      t.string :name, null: false
      t.string :email
      t.integer :age
      t.timestamps
    end

    # Add unique constraint
    ActiveRecord::Base.connection.add_index(error_table, :email, unique: true, name: 'idx_unique_email')
  end

  after do
    ActiveRecord::Base.connection.drop_table(error_table, if_exists: true)
  end
end

RSpec.shared_context 'with performance testing data' do
  let(:performance_table) { :performance_test }
  let(:record_count) { 1000 }

  before do
    ActiveRecord::Base.connection.create_table(performance_table) do |t|
      t.string :name
      t.integer :value
      t.string :category
      t.decimal :score, precision: 10, scale: 2
      t.boolean :active
      t.timestamps
    end

    # Create indexes for performance testing
    ActiveRecord::Base.connection.add_index(performance_table, :category)
    ActiveRecord::Base.connection.add_index(performance_table, %i[category active])
    ActiveRecord::Base.connection.add_index(performance_table, :score)

    # Bulk insert test data
    records = Array.new(record_count) do |i|
      {
        name: "Record #{i}",
        value: i,
        category: %w[A B C D E][i % 5],
        score: BigDecimal((rand * 1000).round(2).to_s),
        active: i.even?,
        created_at: Time.current,
        updated_at: Time.current
      }
    end

    # Use raw SQL for faster bulk insert
    columns = records.first.keys
    values_sql = records.map do |record|
      "(#{columns.map { |col| ActiveRecord::Base.connection.quote(record[col]) }.join(", ")})"
    end.join(', ')

    sql = "INSERT INTO #{performance_table} (#{columns.join(", ")}) VALUES #{values_sql}"
    ActiveRecord::Base.connection.execute(sql)
  end

  after do
    ActiveRecord::Base.connection.drop_table(performance_table, if_exists: true)
  end
end

RSpec.shared_context 'with mocked Rails environment' do
  before do
    stub_const('Rails', Class.new) unless defined?(Rails)

    allow(Rails).to receive_messages(logger: instance_double(Logger,
                                                             debug: nil,
                                                             info: nil,
                                                             warn: nil,
                                                             error: nil,
                                                             fatal: nil), env: instance_double(Environment,
                                                                                               test?: true,
                                                                                               development?: false,
                                                                                               production?: false), root: Pathname.new('/tmp'))
  end
end

RSpec.shared_context 'with temporary files' do
  let(:temp_files) { [] }
  let(:temp_directories) { [] }

  def create_temp_file(extension = '.tmp', content = nil)
    require 'tempfile'
    file = Tempfile.new(['test', extension])
    file.write(content) if content
    file.rewind
    temp_files << file
    file
  end

  def create_temp_directory
    require 'tmpdir'
    dir = Dir.mktmpdir('test')
    temp_directories << dir
    dir
  end

  after do
    temp_files.each do |file|
      file.close!
    rescue StandardError
      nil
    end
    temp_directories.each do |dir|
      FileUtils.rm_rf(dir)
    rescue StandardError
      nil
    end
  end
end

RSpec.shared_context 'with connection pooling' do
  let(:pool_size) { 5 }
  let(:pool_config) { memory_config.merge(pool: pool_size) }

  before do
    ActiveRecord::Base.establish_connection(pool_config)
  end

  after do
    ActiveRecord::Base.remove_connection
  end
end

RSpec.shared_context 'with transaction testing' do
  let(:transaction_table) { :transaction_test }

  before do
    ActiveRecord::Base.connection.create_table(transaction_table) do |t|
      t.string :name
      t.integer :value
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(transaction_table, if_exists: true)
  end

  def create_transaction_model
    Class.new(ActiveRecord::Base) do
      self.table_name = :transaction_test
      validates :name, presence: true
    end
  end
end

RSpec.shared_context 'with concurrent testing setup' do
  let(:thread_count) { 5 }
  let(:iterations_per_thread) { 10 }
  let(:concurrent_table) { :concurrent_test }

  before do
    ActiveRecord::Base.connection.create_table(concurrent_table) do |t|
      t.string :thread_name
      t.integer :iteration
      t.integer :value
      t.timestamps
    end
  end

  after do
    ActiveRecord::Base.connection.drop_table(concurrent_table, if_exists: true)
  end
end
