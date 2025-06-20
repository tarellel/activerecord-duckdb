# frozen_string_literal: true

require 'duckdb'
require 'active_record'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/duckdb/column'
require 'active_record/connection_adapters/duckdb/database_limits'
require 'active_record/connection_adapters/duckdb/database_statements'
require 'active_record/connection_adapters/duckdb/quoting'
require 'active_record/connection_adapters/duckdb/schema_creation'
require 'active_record/connection_adapters/duckdb/schema_statements'
require 'active_record/connection_adapters/duckdb/schema_definitions'
require 'active_record/connection_adapters/duckdb/schema_dumper'

# Inspired by the SQLite adapter
# duckdb: https://github.com/duckdb/duckdb-ruby
# sqlite3 adapter: https://github.com/rails/rails/blob/main/activerecord/lib/active_record/connection_adapters/sqlite3_adapter.rb

module ActiveRecord
  module ConnectionHandling
    # Establishes a connection to a DuckDB database
    # @param config [Hash] Database configuration options
    # @return [ActiveRecord::ConnectionAdapters::DuckdbAdapter] The database adapter instance
    # @raise [ActiveRecord::ConnectionNotEstablished] If connection fails
    def duckdb_connection(config)
      config = config.symbolize_keys
      begin
        # Create adapter first, then let it establish connection
        adapter = ConnectionAdapters::DuckdbAdapter.new(nil, logger, {}, config)
        adapter.send(:connect)
        adapter
      rescue StandardError => e
        raise ActiveRecord::ConnectionNotEstablished,
              "Could not connect to DuckDB database: #{e.message}"
      end
    end
  end

  module ConnectionAdapters
    # This adapter provides a connection to a DuckDB database.
    class DuckdbAdapter < AbstractAdapter
      ADAPTER_NAME = 'DuckDB'

      include Duckdb::DatabaseLimits
      include Duckdb::DatabaseStatements
      include Duckdb::Quoting
      include Duckdb::SchemaStatements
      include Duckdb::SchemaDumper

      # Allow customization of primary key type like PostgreSQL and MySQL adapters do
      class_attribute :primary_key_type, default: :bigint

      # DB configuration if used in memory mode
      MEMORY_MODE_KEYS = [:memory, 'memory', ':memory:', ':memory'].freeze

      # https://duckdb.org/docs/stable/sql/data_types/overview.html
      NATIVE_DATABASE_TYPES = {
        primary_key: 'INTEGER PRIMARY KEY',
        string: { name: 'VARCHAR' },
        integer: { name: 'INTEGER' },
        float: { name: 'REAL' },
        decimal: { name: 'DECIMAL' },
        datetime: { name: 'TIMESTAMP' },
        time: { name: 'TIME' },
        date: { name: 'DATE' },
        bigint: { name: 'BIGINT' },
        binary: { name: 'BLOB' },
        boolean: { name: 'BOOLEAN' },
        uuid: { name: 'UUID' }
      }.freeze

      # Initializes a new DuckDB adapter instance
      # @param args [Array] Arguments passed to the parent AbstractAdapter
      def initialize(*args)
        super
      end

      # Reconnects to the DuckDB database by disconnecting and connecting again
      # @return [void]
      def reconnect
        disconnect
        connect
      end

      # Disconnects from the DuckDB database and cleans up the connection
      # @return [void]
      def disconnect
        @connection&.close
        @connection = nil
      end

      # Checks if the database connection is active
      # @return [Boolean] true if connection is active, false otherwise
      def active?
        !!(@raw_connection || @connection)
      end

      # Returns the raw DuckDB connection object
      # @return [DuckDB::Connection, nil] The raw connection object or nil if not connected
      def raw_connection
        @raw_connection || @connection
      end

      class << self
        # Opens the DuckDB command line console
        # @param config [ActiveRecord::DatabaseConfigurations::DatabaseConfig] Database configuration
        # @param options [Hash] Additional options for the console
        # @return [void]
        def dbconsole(config, options = {})
          db_config = config.configuration_hash
          args = []
          args << db_config[:database] if db_config[:database] && !MEMORY_MODE_KEYS.include?(db_config[:database])

          find_cmd_and_exec('duckdb', *args)
        end

        # Checks if the DuckDB database file exists
        # @param config [Hash] Database configuration containing database path
        # @return [Boolean] true if database exists or is in-memory, false otherwise
        def database_exists?(config)
          # Logic to check if database exists
          database_path = config[:database]
          return true if MEMORY_MODE_KEYS.include?(database_path)

          File.exist?(database_path.to_s)
        end
      end

      # Indicates whether the adapter supports INSERT...RETURNING syntax
      # @return [Boolean] always returns true for DuckDB
      def use_insert_returning?
        true
      end

      # Indicates whether the adapter supports INSERT RETURNING for Rails 8
      # @return [Boolean] always returns true for DuckDB
      def supports_insert_returning?
        true
      end

      # Indicates whether the adapter supports INSERT ON DUPLICATE SKIP syntax
      # @return [Boolean] always returns false for DuckDB
      def supports_insert_on_duplicate_skip?
        false
      end

      # Determines if a column value should be returned after insert
      # @param column [ActiveRecord::ConnectionAdapters::Column] The column to check
      # @return [Boolean] true if column has default function or parent method returns true
      def return_value_after_insert?(column)
        column.default_function.present? || super
      end

      # Indicates whether the adapter supports INSERT ON DUPLICATE UPDATE syntax
      # @return [Boolean] always returns false for DuckDB
      def supports_insert_on_duplicate_update?
        false
      end

      # Determines if primary key should be prefetched before insert
      # @param _table_name [String] The table name (unused)
      # @return [Boolean] always returns false to exclude auto-increment columns from INSERT
      def prefetch_primary_key?(_table_name)
        false
      end

      # Returns the sequence name for a serial column
      # @param table [String] The table name
      # @param column [String] The column name
      # @return [nil] always returns nil as DuckDB doesn't use named sequences
      def serial_sequence(table, column)
        # Return sequence name if using sequences, nil otherwise
        nil
      end

      # Returns the primary key columns for a table
      # @param table_name [String] The name of the table
      # @return [Array<String>] Array of primary key column names
      # @raise [ArgumentError] if table_name is blank
      def primary_keys(table_name) # :nodoc:
        raise ArgumentError, 'table_name cannot be blank' unless table_name.present?

        results = execute("PRAGMA table_info(#{quote(table_name.to_s)})", 'SCHEMA')
        results.filter_map do |result|
          _cid, name, _type, _notnull, _dflt_value, pk = result

          # pk can be true, false, 1, 0, or nil in DuckDB PRAGMA table_info
          # true or 1 means it's a primary key, false, 0, or nil means it's not
          pk_value = pk == true ? 1 : pk
          [pk_value, name] if [true, 1].include?(pk)
        end.sort_by(&:first).map(&:last)
      end

      # Returns the native database types supported by DuckDB
      # @return [Hash] Hash mapping ActiveRecord types to DuckDB native types
      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      # Returns column definitions for a table using PRAGMA table_info
      # @param table_name [String] The name of the table
      # @return [Array<Array>] Array of column definition arrays
      def column_definitions(table_name)
        # Use PRAGMA table_info which gives us more accurate type information
        # including primary key detection that information_schema might miss
        pragma_results = execute("PRAGMA table_info(#{quote(table_name.to_s)})", 'SCHEMA')
        pragma_results.map do |row|
          _cid, column_name, data_type, not_null, column_default, pk = row

          # Format the type properly - DuckDB PRAGMA gives us the actual type
          # Preserve VARCHAR limits and other type information
          formatted_type = case data_type.to_s.upcase
                           when /^BIGINT$/i
                             'BIGINT'
                           when /^INTEGER$/i
                             'INTEGER'
                           when /^VARCHAR$/i, /^VARCHAR\(\d+\)$/i
                             data_type.to_s  # Preserve the full VARCHAR(n) format
                           when /^DECIMAL\(\d+,\d+\)$/i
                             data_type.to_s  # Preserve DECIMAL(p,s) format
                           when /^TIMESTAMP$/i
                             'TIMESTAMP'
                           when /^BOOLEAN$/i
                             'BOOLEAN'
                           when /^UUID$/i
                             'UUID'
                           when /^BLOB$/i
                             'BLOB'
                           when /^DATE$/i
                             'DATE'
                           when /^TIME$/i
                             'TIME'
                           when /^REAL$/i, /^DOUBLE$/i
                             data_type.to_s
                           else
                             data_type.to_s
                           end

          # Convert PRAGMA results to match information_schema format
          [
            column_name,           # column_name
            formatted_type,        # formatted_type
            column_default,        # column_default
            not_null == 1,         # not_null (true if NOT NULL constraint)
            nil,                   # type_id
            nil,                   # type_modifier
            nil,                   # collation_name
            nil,                   # comment
            nil,                   # identity
            nil,                   # generated
            pk == 1                # primary_key flag (true if primary key)
          ]
        end
      end

      # Indicates whether the adapter supports a specific primary key type
      # Support Rails id: :uuid convention
      # @param type [Symbol] The primary key type to check
      # @return [Boolean] true if the type is supported, false otherwise
      def supports_primary_key_type?(type)
        case type
        when :uuid, :string, :integer, :bigint, :primary_key
          true
        else
          false
        end
      end

      # Generates SQL for getting the next sequence value
      # @param sequence_name [String] The name of the sequence
      # @return [String] SQL expression for next sequence value
      def next_sequence_value(sequence_name)
        "nextval('#{sequence_name}')"
      end

      # Generates default sequence name following PostgreSQL/Oracle conventions
      # @param table_name [String] The name of the table
      # @param column_name [String] The name of the column (defaults to 'id')
      # @return [String] The generated sequence name
      def default_sequence_name(table_name, column_name = 'id')
        "#{table_name}_#{column_name}_seq"
      end

      # Configures the primary key type at the class level
      # @param type [Symbol] The primary key type to set
      # @return [Symbol] The configured primary key type
      def self.configure_primary_key_type(type)
        self.primary_key_type = type
      end

      # Returns the primary key column name for a table
      # @param table_name [String] The name of the table
      # @return [String, nil] The primary key column name, or nil if no single primary key
      def primary_key(table_name)
        pk_columns = primary_keys(table_name)
        pk_columns.size == 1 ? pk_columns.first : nil
      end

      # Override to prevent Rails schema dumper from detecting sequence defaults as table defaults
      # @param table_name [String, Symbol] The name of the table
      # @return [nil] Always returns nil to prevent table-level defaults
      def primary_key_definition(table_name)
        # Always return nil to prevent any table-level defaults from sequence columns
        nil
      end

      # Override to ensure sequence defaults never appear at table level
      # @param table_name [String, Symbol] The name of the table
      # @return [nil] Always returns nil as defaults belong to columns
      def table_default_value(table_name)
        # Never return defaults at table level - they belong to columns
        nil
      end

      # Returns default value for table in schema dumping
      # @param table_name [String] The name of the table
      # @return [nil] Always returns nil to prevent sequence defaults at table level
      def default_value_for_table(table_name)
        nil
      end

      # Maps DuckDB column types to ActiveRecord schema types
      # Override schema dumping to fix type detection
      # @param column [ActiveRecord::ConnectionAdapters::Column] The column object
      # @return [Symbol] The Rails schema type for the column
      def schema_type(column)
        case column.sql_type
        when /^BIGINT$/i
          :bigint
        when /^INTEGER$/i
          :integer
        when /^VARCHAR$/i, /^VARCHAR\(\d+\)$/i
          :string
        when /^TIMESTAMP$/i
          :datetime
        when /^BOOLEAN$/i
          :boolean
        when /^UUID$/i
          :uuid
        else
          super
        end
      end

      # Returns indexes for a table or all tables
      # @param table_name [String, nil] The table name, or nil for all tables
      # @return [Array] Array of index definitions
      def indexes(table_name = nil)
        if table_name
          # Delegate to the schema statements implementation
          super
        else
          # This shouldn't happen in normal schema dumping, but handle gracefully
          []
        end
      end

      # Determines the primary key type for schema dumping
      # @param table_name [String] The name of the table
      # @return [Symbol, nil] The primary key type symbol, or nil if no primary key
      def primary_key_type_for_schema_dump(table_name)
        pk_column = primary_key(table_name)
        return nil unless pk_column

        # Get the actual column definition
        col_def = columns(table_name).find { |c| c.name == pk_column }
        return nil unless col_def

        case col_def.sql_type.to_s.upcase
        when 'BIGINT'
          :bigint
        when 'INTEGER'
          :integer
        when 'UUID'
          :uuid
        when /^VARCHAR/
          :string
        end
      end

      # Returns table options for schema dumping
      # @param table_name [String] The name of the table
      # @return [Hash] Hash of table options for schema dumping
      def table_options(table_name)
        options = {}

        # Check if primary key has sequence default
        pk_name = primary_key(table_name)
        pk_column = pk_name ? columns(table_name).find { |c| c.name == pk_name } : nil
        has_sequence_default = pk_column&.default_function&.include?('nextval(')

        pk_type = primary_key_type_for_schema_dump(table_name)
        if has_sequence_default
          # Force explicit primary key inclusion when sequence is involved
          # This prevents Rails from putting sequence default at table level
          options[:id] = pk_type || :bigint
        elsif pk_type && pk_type != :bigint
          # Set the correct primary key type only when different from default
          options[:id] = pk_type # Rails 5+ default is bigint
        end

        # NEVER include defaults at table level - sequence defaults belong to columns
        options
      end

      # Check if a type is valid for schema dumping
      # @param type [Symbol, String] The type to validate
      # @return [Boolean] true if type is valid for DuckDB schema dumping
      def valid_type?(type)
        case type.to_s.to_sym
        when :string, :text, :integer, :bigint, :float, :decimal, :datetime, :timestamp,
             :time, :date, :binary, :boolean, :uuid, :interval, :bit,
             :hugeint, :tinyint, :smallint, :utinyint, :usmallint, :uinteger, :ubigint, :uhugeint,
             :varint, :blob, :list, :struct, :map, :enum, :union, :real, :double, :numeric
          true
        else
          false
        end
      end

      private

      # Establishes the actual connection to the DuckDB database
      # @return [void]
      def connect
        database = @config[:database] || :memory
        db = if MEMORY_MODE_KEYS.include?(database)
               DuckDB::Database.open
             else
               DuckDB::Database.open(database)
             end
        @raw_connection = db.connect
      end
    end
  end
end
