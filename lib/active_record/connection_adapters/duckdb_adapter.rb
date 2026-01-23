# frozen_string_literal: true

require 'duckdb'
require 'active_record'
require 'active_record/connection_adapters/abstract_adapter'

require 'active_record/connection_adapters/duckdb/timestamp_monkey_patch'
require 'active_record/connection_adapters/duckdb/column'
require 'active_record/connection_adapters/duckdb/type/interval'
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
  module ConnectionAdapters
    # Raised when attempting to use savepoints with DuckDB, which does not support them.
    class SavepointsNotSupported < NotImplementedError
      def initialize
        super('DuckDB does not support savepoints. Avoid using transaction(requires_new: true) or nested transactions that rely on savepoint isolation.')
      end
    end
  end

  module ConnectionHandling
    # Establishes a connection to a DuckDB database
    #
    # @deprecated This method is provided for legacy compatibility only.
    #   In Rails 8+, adapters are registered via ActiveRecord::ConnectionAdapters.register
    #   and connections are created by calling AdapterClass.new(config) directly.
    #   Use ActiveRecord::Base.establish_connection instead for standard Rails usage.
    #
    # @param config [Hash] Database configuration options
    # @return [ActiveRecord::ConnectionAdapters::DuckdbAdapter] The database adapter instance
    # @raise [ActiveRecord::ConnectionNotEstablished] If connection fails
    def duckdb_connection(config)
      config = config.symbolize_keys
      begin
        # Create adapter and establish connection via Rails lifecycle
        # connect! calls verify! which calls reconnect! and configure_connection
        adapter = ConnectionAdapters::DuckdbAdapter.new(nil, logger, {}, config)
        adapter.connect!
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

      # Include Rails version-specific database statements.
      # Rails 8.0+: Use raw_execute, let base class handle internal_exec_query.
      # Rails 7.2: Must implement internal_exec_query directly.
      if ActiveRecord::VERSION::MAJOR >= 8
        require 'active_record/connection_adapters/duckdb/database_statements_rails8'
        include Duckdb::DatabaseStatementsRails8
      else
        require 'active_record/connection_adapters/duckdb/database_statements_rails72'
        include Duckdb::DatabaseStatementsRails72
      end

      # Include Rails version-specific schema statements.
      # Rails 8.1+: Column constructor includes cast_type parameter.
      # Rails 7.2/8.0: Column constructor without cast_type parameter.
      if ActiveRecord::VERSION::MAJOR > 8 ||
         (ActiveRecord::VERSION::MAJOR == 8 && ActiveRecord::VERSION::MINOR >= 1)
        require 'active_record/connection_adapters/duckdb/schema_statements_rails81'
        include Duckdb::SchemaStatementsRails81
      else
        require 'active_record/connection_adapters/duckdb/schema_statements_rails80'
        include Duckdb::SchemaStatementsRails80
      end

      # Allow customization of primary key type like PostgreSQL and MySQL adapters do
      class_attribute :primary_key_type, default: :bigint

      # DB configuration if used in memory mode
      MEMORY_MODE_KEYS = [:memory, 'memory', ':memory:', ':memory'].freeze

      class << self
        # Creates a new DuckDB database connection
        # @param config [Hash] Database configuration
        # @return [DuckDB::Connection] The raw database connection
        def new_client(config)
          database = config[:database] || :memory
          db = if MEMORY_MODE_KEYS.include?(database)
                 DuckDB::Database.open
               else
                 DuckDB::Database.open(database.to_s)
               end
          db.connect
        end

        private

        # Initializes the type map with DuckDB-specific type mappings
        # @param m [ActiveRecord::Type::TypeMap] The type map to initialize
        def initialize_type_map(m)
          m.register_type(/^boolean$/i,    Type::Boolean.new)
          m.register_type(/^date$/i,       Type::Date.new)
          m.register_type(/^time$/i,       Type::Time.new)
          m.register_type(/^timestamp$/i,  Type::DateTime.new)
          m.register_type(/^datetime$/i,   Type::DateTime.new)
          m.register_type(/^float$/i,      Type::Float.new)
          m.register_type(/^real$/i,       Type::Float.new)
          m.register_type(/^double$/i,     Type::Float.new)

          # Integer types with proper byte limits
          m.register_type(/^tinyint$/i)    { Type::Integer.new(limit: 1) }
          m.register_type(/^smallint$/i)   { Type::Integer.new(limit: 2) }
          m.register_type(/^integer$/i)    { Type::Integer.new(limit: 4) }
          # BigInteger handles unlimited bytes
          m.register_type(/^bigint$/i)     { Type::BigInteger.new }
          m.register_type(/^hugeint$/i)    { Type::BigInteger.new }

          # Unsigned integer types
          m.register_type(/^utinyint$/i)   { Type::UnsignedInteger.new(limit: 1) }
          m.register_type(/^usmallint$/i)  { Type::UnsignedInteger.new(limit: 2) }
          m.register_type(/^uinteger$/i)   { Type::UnsignedInteger.new(limit: 4) }
          m.register_type(/^ubigint$/i)    { Type::UnsignedInteger.new(limit: 8) }
          m.register_type(/^uhugeint$/i)   { Type::BigInteger.new }

          # String types
          m.register_type(/^varchar/i,     Type::String.new)
          m.register_type(/^text$/i,       Type::Text.new)
          m.register_type(/^uuid$/i,       Type::String.new)

          # Binary
          m.register_type(/^blob$/i,       Type::Binary.new)
          m.register_type(/^bytea$/i,      Type::Binary.new)

          # Decimal with precision/scale
          m.register_type(/^decimal/i)     { Type::Decimal.new }
          m.register_type(/^numeric/i)     { Type::Decimal.new }

          # Interval type - maps to ActiveSupport::Duration
          m.register_type(/^interval$/i)   { Duckdb::Type::Interval.new }
        end
      end

      # Type map for DuckDB SQL types to ActiveRecord types
      TYPE_MAP = Type::TypeMap.new.tap { |m| initialize_type_map(m) }

      # https://duckdb.org/docs/stable/sql/data_types/overview.html
      # Integer limits (in bytes): tinyint=1, smallint=2, integer=4, bigint=8
      NATIVE_DATABASE_TYPES = {
        primary_key: 'INTEGER PRIMARY KEY',
        string: { name: 'VARCHAR' },
        integer: { name: 'INTEGER', limit: 4 },
        float: { name: 'REAL' },
        decimal: { name: 'DECIMAL' },
        datetime: { name: 'TIMESTAMP' },
        time: { name: 'TIME' },
        date: { name: 'DATE' },
        bigint: { name: 'BIGINT', limit: 8 },
        binary: { name: 'BLOB' },
        boolean: { name: 'BOOLEAN' },
        uuid: { name: 'UUID' },
        # DuckDB-specific signed integer types
        tinyint: { name: 'TINYINT', limit: 1 },
        smallint: { name: 'SMALLINT', limit: 2 },
        hugeint: { name: 'HUGEINT' },
        # DuckDB-specific unsigned integer types
        utinyint: { name: 'UTINYINT', limit: 1 },
        usmallint: { name: 'USMALLINT', limit: 2 },
        uinteger: { name: 'UINTEGER', limit: 4 },
        ubigint: { name: 'UBIGINT', limit: 8 },
        uhugeint: { name: 'UHUGEINT' },
        # Other DuckDB types
        interval: { name: 'INTERVAL' }
      }.freeze

      # Settings that MUST be applied before loading extensions
      EARLY_SETTINGS = %i[allow_persistent_secrets allow_community_extensions].freeze

      # Default DuckDB settings for secure and predictable behavior
      # Note: lock_configuration is handled separately at the end of configure_connection
      DEFAULT_SETTINGS = {
        allow_persistent_secrets: false,
        allow_community_extensions: false,
        autoinstall_known_extensions: false,
        autoload_known_extensions: false,
        threads: 1,
        memory_limit: '1GiB',
        max_temp_directory_size: '4GiB'
      }.freeze

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

      # Looks up the cast type for a given SQL type string
      # Uses the DuckDB TYPE_MAP to return appropriate ActiveRecord types
      # This ensures BIGINT columns use BigInteger type for full 8-byte range support
      # @param sql_type [String] The SQL type string (e.g., 'BIGINT', 'INTEGER')
      # @return [ActiveRecord::Type::Value] The corresponding ActiveRecord type
      def lookup_cast_type(sql_type)
        TYPE_MAP.lookup(sql_type)
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
      # DuckLake does NOT support INSERT...RETURNING, so we check for DuckLake mode
      # @return [Boolean] true for regular DuckDB, false for DuckLake
      def use_insert_returning?
        !ducklake?
      end

      # Indicates whether the adapter supports INSERT RETURNING for Rails 8
      # DuckLake does NOT support INSERT...RETURNING, so we check for DuckLake mode
      # @return [Boolean] true for regular DuckDB, false for DuckLake
      def supports_insert_returning?
        !ducklake?
      end

      # Detects if the current database is a DuckLake database
      # Uses a metadata query to check the database type
      # @return [Boolean] true if current database is DuckLake, false otherwise
      def ducklake?
        return @ducklake if defined?(@ducklake)

        @ducklake = begin
          with_raw_connection do |conn|
            result = conn.query('SELECT type FROM duckdb_databases() WHERE database_name = current_database()')
            db_type = result.first&.first
            db_type.to_s.downcase == 'ducklake'
          end
        rescue DuckDB::Error
          false
        end
      end

      # Checks if the DuckLake extension is available and can be loaded
      # @return [Boolean] true if DuckLake extension is available, false otherwise
      def ducklake_extension_available?
        return @ducklake_extension_available if defined?(@ducklake_extension_available)

        @ducklake_extension_available = begin
          with_raw_connection do |conn|
            conn.execute('INSTALL ducklake')
            conn.execute('LOAD ducklake')
          end
          true
        rescue DuckDB::Error
          false
        end
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

      # DuckDB does not support savepoints at the SQL level.
      # @return [Boolean] always returns false
      def supports_savepoints? = false

      # DuckDB does not support savepoints.
      # @param _name [String] The savepoint name (ignored)
      # @raise [SavepointsNotSupported] always raises since DuckDB doesn't support savepoints
      def create_savepoint(_name = nil) = raise SavepointsNotSupported

      # DuckDB does not support savepoints.
      # @param _name [String] The savepoint name (ignored)
      # @raise [SavepointsNotSupported] always raises since DuckDB doesn't support savepoints
      def exec_rollback_to_savepoint(_name = nil) = raise SavepointsNotSupported

      # DuckDB does not support savepoints.
      # @param _name [String] The savepoint name (ignored)
      # @raise [SavepointsNotSupported] always raises since DuckDB doesn't support savepoints
      def release_savepoint(_name = nil) = raise SavepointsNotSupported

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
      # DuckLake doesn't support PRIMARY KEY constraints, so we return a modified version
      # @return [Hash] Hash mapping ActiveRecord types to DuckDB native types
      def native_database_types
        return NATIVE_DATABASE_TYPES unless ducklake?

        # DuckLake doesn't support PRIMARY KEY/UNIQUE constraints
        @ducklake_database_types ||= NATIVE_DATABASE_TYPES.merge(
          primary_key: 'INTEGER'
        )
      end

      # Configures the DuckDB connection with extensions, settings, secrets, and attachments.
      # DuckDB locks configuration permanently after initial setup, so we skip reconfiguration
      # if the connection is already configured. This is necessary because Rails/test-prof
      # may call reset! which triggers configure_connection again.
      # @return [void]
      def configure_connection
        # Skip reconfiguration if already configured - DuckDB locks configuration permanently
        return if configuration_locked?

        super
        apply_early_settings
        install_extensions
        apply_settings
        create_secrets
        attach_databases
        use_database
        lock_configuration
      end

      # Checks if DuckDB configuration is locked.
      # Once lock_configuration is set to true, no configuration changes can be made.
      # @return [Boolean] true if configuration is locked
      def configuration_locked?
        return false unless raw_connection

        result = raw_connection.query("SELECT current_setting('lock_configuration')")
        result.first&.first == true
      rescue DuckDB::Error
        false
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
          # Note: DuckLake returns booleans (true/false) while regular DuckDB may return integers (1/0)
          [
            column_name,                  # column_name
            formatted_type,               # formatted_type
            column_default,               # column_default
            not_null.in?([1, true]),      # not_null (true if NOT NULL constraint)
            nil,                          # type_id
            nil,                          # type_modifier
            nil,                          # collation_name
            nil,                          # comment
            nil,                          # identity
            nil,                          # generated
            pk.in?([1, true])             # primary_key flag (true if primary key)
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
        "nextval(#{quote(sequence_name)})"
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

      # Creates a schema dumper instance for DuckDB
      # Uses the DuckDB-specific SchemaDumper class to handle DuckDB types and DuckLake features
      # @param options [Hash] Schema dumper options
      # @return [Duckdb::SchemaDumper] The schema dumper instance
      def create_schema_dumper(options) # :nodoc:
        Duckdb::SchemaDumper.create(self, options)
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

      # Reconnects to the database by closing existing connection and establishing new one.
      # Called by Rails' reconnect! which will also call configure_connection.
      # We reset the @duckdb_configured flag so the new connection gets configured.
      # @return [void]
      def reconnect
        @raw_connection&.close
        @duckdb_configured = false  # Reset so configure_connection will run on new connection
        remove_instance_variable(:@ducklake) if defined?(@ducklake)  # Reset ducklake detection
        connect
      end

      # Establishes the actual connection to the DuckDB database
      # @return [void]
      def connect
        @raw_connection = self.class.new_client(@config)
      end

      # Returns merged settings (defaults + user config)
      # @return [Hash] Merged settings hash
      def merged_settings
        @merged_settings ||= DEFAULT_SETTINGS.merge((@config[:settings] || {}).transform_keys(&:to_sym))
      end

      # Applies settings that must be set before loading extensions
      # @return [void]
      def apply_early_settings
        merged_settings.each do |key, value|
          next unless EARLY_SETTINGS.include?(key)

          execute_setting(key, value)
        end
      end

      # Installs and loads configured extensions
      # @return [void]
      def install_extensions
        extensions = @config[:extensions] || []
        extensions.each do |extension|
          raw_connection.execute("INSTALL #{extension}")
          raw_connection.execute("LOAD #{extension}")
        end
      end

      # Applies settings that can be set after loading extensions
      # @return [void]
      def apply_settings
        merged_settings.each do |key, value|
          next if EARLY_SETTINGS.include?(key)

          execute_setting(key, value)
        end
      end

      # Executes a single SET statement
      # @param key [Symbol, String] Setting name
      # @param value [Object] Setting value
      # @return [void]
      def execute_setting(key, value)
        formatted_value = case value
                          when String then "'#{value}'"
                          when TrueClass, FalseClass then value.to_s
                          else value.to_s
                          end
        raw_connection.execute("SET #{key} = #{formatted_value}")
      end

      # Creates secrets from configuration
      # If a secret has an explicit 'type' key, the hash key becomes the secret name
      # Otherwise, the hash key is the secret type (unnamed secret)
      # @return [void]
      def create_secrets
        secrets = @config[:secrets] || {}
        secrets.each do |key, fields|
          fields = fields.transform_keys(&:to_sym)
          type = fields.delete(:type)
          name = type ? key : nil # Named secret: key is the name, type is explicit
          type ||= key # Unnamed secret: key is the type
          formatted_fields = format_secret_fields(fields)
          sql = +'CREATE SECRET'
          sql << " #{name}" if name
          sql << " (TYPE #{type}"
          sql << ", #{formatted_fields}" unless formatted_fields.empty?
          sql << ')'
          raw_connection.execute(sql)
        end
      end

      # Formats secret fields for CREATE SECRET statement
      # @param fields [Hash] Secret fields
      # @return [String] Formatted fields string
      def format_secret_fields(fields)
        fields.filter_map do |key, value|
          next if value.nil?

          formatted_value = case value
                            when Integer then value.to_s
                            else "'#{value}'"
                            end
          "#{key.to_s.upcase} #{formatted_value}"
        end.join(', ')
      end

      # Attaches configured databases
      # @return [void]
      def attach_databases
        attachments = @config[:attachments] || []
        attachments.each do |attachment|
          attachment = attachment.transform_keys(&:to_sym)
          name = attachment[:name]
          connection_string = attachment[:connection_string]
          type = attachment[:type]
          options = attachment[:options]

          sql = "ATTACH '#{connection_string}' AS #{name}"
          params = []
          params << "TYPE #{type}" if type
          params << options if options
          sql += " (#{params.join(", ")})" unless params.empty?

          raw_connection.execute(sql)
        end
      end

      # Sets the active database using USE statement
      # This is only needed when you want to switch to a different attached database
      # The main database opened via DuckDB::Database.open is already active
      # @return [void]
      def use_database
        # The use_database config option allows switching to a specific attached database
        # This is NOT the same as the database config option which specifies the file to open
        use_db = @config[:use_database]
        return if use_db.nil?

        raw_connection.execute("USE #{use_db}")
        # Reset ducklake? memoization since current_database has changed
        remove_instance_variable(:@ducklake) if defined?(@ducklake)
      end

      # Locks the DuckDB configuration to prevent further changes
      # This should be called at the very end of configure_connection
      # @return [void]
      def lock_configuration
        raw_connection.execute('SET lock_configuration = true')
      end
    end
  end
end
