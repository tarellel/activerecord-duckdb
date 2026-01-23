# frozen_string_literal: true

require 'active_record/connection_adapters/abstract/schema_statements'
require 'duckdb'

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific schema statement implementations
      # Provides DuckDB-specific functionality for table, sequence, and index management
      module SchemaStatements
        # Returns a DuckDB-specific schema creation instance
        # @return [ActiveRecord::ConnectionAdapters::Duckdb::SchemaCreation] Schema creation helper
        def schema_creation
          SchemaCreation.new(self)
        end

        # Returns a list of all tables in the database
        # @return [Array<String>] Array of table names
        def tables
          result = execute(data_source_sql(type: 'BASE TABLE'), 'SCHEMA')
          result.to_a.map { |row| row[0] }
        end

        # Returns a list of all views in the database
        # @return [Array<String>] Array of view names
        def views
          result = execute(data_source_sql(type: 'VIEW'), 'SCHEMA')
          result.to_a.map { |row| row[0] }
        end

        # Returns options for internal primary key columns (schema_migrations, ar_internal_metadata)
        # DuckLake doesn't support PRIMARY KEY constraints, so we omit them in that mode
        # @return [Hash] Options hash for string primary key columns
        def internal_string_options_for_primary_key
          # DuckLake doesn't support PRIMARY KEY/UNIQUE constraints
          ducklake? ? {} : { primary_key: true }
        end

        # Creates a DuckDB-specific table definition instance
        # @param name [String, Symbol] The table name
        # @return [ActiveRecord::ConnectionAdapters::Duckdb::TableDefinition] Table definition instance
        def create_table_definition(name, **)
          TableDefinition.new(self, name, **)
        end

        # Creates a table with DuckDB-specific handling for sequences and primary keys
        # @param table_name [String, Symbol] The name of the table to create
        # @param id [Symbol, Boolean] The primary key type or false for no primary key
        # @param primary_key [String, Symbol, nil] Custom primary key column name
        # @param options [Hash] Additional table creation options
        # @return [void]
        def create_table(table_name, id: :primary_key, primary_key: nil, **options)
          # Handle sequence creation for integer primary keys BEFORE table creation
          sequence_name = nil
          pk_column_name = nil
          needs_sequence_default = false
          if id != false && id != :uuid && id != :string
            pk_column_name = primary_key || 'id'
            sequence_name = "#{table_name}_#{pk_column_name}_seq"
            needs_sequence_default = true
            # Extract sequence start value from options
            start_with = options.dig(:sequence, :start_with) || options[:start_with] || 1
            create_sequence_safely(sequence_name, table_name, start_with: start_with)
          end

          # Store sequence info for later use during table creation
          @pending_sequence_default = ({ table: table_name, column: pk_column_name, sequence: sequence_name } if needs_sequence_default && sequence_name && pk_column_name)

          begin
            # Now create the table with Rails handling the standard creation
            super do |td|
              # If block given, let user define columns
              yield td if block_given?
            end
          ensure
            # Clear the pending sequence default
            @pending_sequence_default = nil
          end
        end

        # Creates a sequence in DuckDB
        # @param sequence_name [String] The name of the sequence to create
        # @param start_with [Integer] The starting value for the sequence (default: 1)
        # @param increment_by [Integer] The increment value for the sequence (default: 1)
        # @param options [Hash] Additional sequence options
        # @return [void]
        def create_sequence(sequence_name, start_with: 1, increment_by: 1, **options)
          sql = "CREATE SEQUENCE #{quote_table_name(sequence_name)}"
          sql << " START #{start_with.to_i}" if start_with != 1
          sql << " INCREMENT #{increment_by.to_i}" if increment_by != 1
          execute(sql, 'Create Sequence')
        end

        # Drops a sequence from the database
        # @param sequence_name [String] The name of the sequence to drop
        # @param if_exists [Boolean] Whether to use IF EXISTS clause (default: false)
        # @return [void]
        def drop_sequence(sequence_name, if_exists: false)
          sql = +'DROP SEQUENCE'
          sql << ' IF EXISTS' if if_exists
          sql << " #{quote_table_name(sequence_name)}"
          execute(sql, 'Drop Sequence')
        end

        # Checks if a sequence exists in the database
        # @param sequence_name [String] The name of the sequence to check
        # @return [Boolean] true if the sequence exists, false otherwise
        def sequence_exists?(sequence_name)
          # Try to get next value from sequence in a way that doesn't consume it
          # Use a transaction that we can rollback to avoid side effects
          transaction do
            execute("SELECT nextval(#{quote(sequence_name)})", 'SCHEMA')
            raise ActiveRecord::Rollback # Rollback to avoid consuming the sequence value
          end
          true
        rescue ActiveRecord::StatementInvalid, DuckDB::Error => e
          # If the sequence doesn't exist, nextval will fail with a specific error
          raise unless e.message.include?('does not exist') || e.message.include?('Catalog Error')

          false

        # Re-raise other types of errors
        rescue StandardError
          # For any other error, assume sequence doesn't exist
          false
        end

        # Returns SQL expression to get the next value from a sequence
        # @param sequence_name [String] The name of the sequence
        # @return [String] SQL expression for getting next sequence value
        def next_sequence_value(sequence_name)
          "nextval(#{quote(sequence_name)})"
        end

        # Resets a sequence to a specific value
        # @param sequence_name [String] The name of the sequence to reset
        # @param value [Integer] The value to reset the sequence to (default: 1)
        # @return [void]
        def reset_sequence!(sequence_name, value = 1)
          execute("ALTER SEQUENCE #{quote_table_name(sequence_name)} RESTART WITH #{value.to_i}", 'Reset Sequence')
        end

        # Returns a list of all sequences in the database
        # @return [Array<String>] Array of sequence names (currently returns empty array)
        def sequences
          # For now, return empty array since DuckDB sequence introspection is limited
          []
        end

        # Sets partitioning for a DuckLake table
        # DuckLake supports time-based partitioning using SQL functions
        # @param table_name [String, Symbol] The name of the table to partition
        # @param expressions [Array<String>] Array of SQL expressions for partitioning
        # @return [void]
        # @example Partition by year, month, day
        #   set_partitioned_by(:datapoints, ['year(created_at)', 'month(created_at)', 'day(created_at)'])
        # @see https://ducklake.select/docs/stable/sql/statements/alter_table.html
        def set_partitioned_by(table_name, expressions)
          sql = "ALTER TABLE #{quote_table_name(table_name)} SET PARTITIONED BY (#{expressions.join(', ')})"
          execute(sql, 'Set Partitioned By')
        end

        # Note: DuckLake does not support removing partitioning after it has been set.
        # Partitioning is a one-way operation. To change partitioning, you must recreate the table.

        # Returns the partition expressions for a DuckLake table
        # @param table_name [String, Symbol] The name of the table
        # @return [Array<String>] Array of partition expressions (e.g., ['month(created_at)'])
        # @return [nil] If the table is not partitioned or not in DuckLake mode
        def partition_expressions(table_name)
          return nil unless ducklake?

          # Find the metadata schema for the current database
          metadata_schema = ducklake_metadata_schema
          return nil unless metadata_schema

          # Query partition info from DuckLake metadata tables
          # Schema:
          #   ducklake_partition_column: partition_id, table_id, partition_key_index, column_id, transform
          #   ducklake_column: column_id, table_id, column_name, ...
          #   ducklake_table: table_id, table_name, ...
          sql = <<~SQL
            SELECT pc.partition_key_index, c.column_name, pc.transform
            FROM #{quote_table_name(metadata_schema)}.ducklake_partition_column pc
            JOIN #{quote_table_name(metadata_schema)}.ducklake_table t ON pc.table_id = t.table_id
            JOIN #{quote_table_name(metadata_schema)}.ducklake_column c ON pc.column_id = c.column_id AND c.table_id = t.table_id
            WHERE t.table_name = #{quote(table_name.to_s)}
            ORDER BY pc.partition_key_index
          SQL

          result = execute(sql, 'Get Partition Expressions')
          expressions = result.map do |row|
            _index, column_name, transform = row
            if transform && !transform.to_s.empty?
              "#{transform}(#{column_name})"
            else
              column_name.to_s
            end
          end

          expressions.empty? ? nil : expressions
        rescue StandardError
          # If metadata tables don't exist or query fails, return nil
          nil
        end

        # Returns the DuckLake metadata schema name for the current database
        # @return [String, nil] The metadata schema name or nil if not found
        def ducklake_metadata_schema
          # Don't memoize - current_database can change after USE DATABASE
          # DuckLake metadata is stored in __ducklake_metadata_<database_name>
          result = execute("SELECT current_database()", 'Get Current Database')
          db_name = result.first&.first
          return nil if db_name.nil? || db_name.empty?

          "__ducklake_metadata_#{db_name}"
        rescue StandardError
          nil
        end

        # Sets a DuckLake-specific option using CALL set_option
        # @param option_name [String] The name of the option to set
        # @param value [String, Integer] The value to set
        # @param table_name [String, Symbol, nil] Optional table name for table-scoped options
        # @return [void]
        # @example Set global parquet version
        #   set_ducklake_option('parquet_version', '2')
        # @example Set table-level parquet compression
        #   set_ducklake_option('parquet_compression', 'zstd', :events)
        def set_ducklake_option(option_name, value, table_name = nil)
          formatted_value = value.is_a?(Integer) ? value.to_s : quote(value)
          if table_name
            sql = "CALL set_option(#{quote(option_name)}, #{formatted_value}, #{quote(table_name.to_s)})"
          else
            sql = "CALL set_option(#{quote(option_name)}, #{formatted_value})"
          end
          execute(sql, 'Set DuckLake Option')
        end

        # DuckLake options that should be included in schema dumps
        # These are user-configurable options, not internal metadata like 'version' or 'created_by'
        # See: https://ducklake.select/docs/stable/specification/tables/ducklake_metadata.html
        DUMPABLE_DUCKLAKE_OPTIONS = %w[
          data_inlining_row_limit
          target_file_size
          parquet_row_group_size_bytes
          parquet_row_group_size
          parquet_compression
          parquet_compression_level
          parquet_version
          hive_file_pattern
          require_commit_message
          rewrite_delete_threshold
          delete_older_than
          expire_older_than
          per_thread_output
          encrypted
        ].freeze

        # Returns DuckLake options for schema dumping
        # @return [Hash] Hash of option_name => value for user-configurable options
        # @return [nil] If not in DuckLake mode or no options set
        def ducklake_options
          return nil unless ducklake?

          metadata_schema = ducklake_metadata_schema
          return nil unless metadata_schema

          # Query the ducklake_metadata table for global options (scope IS NULL)
          sql = <<~SQL
            SELECT key, value
            FROM #{quote_table_name(metadata_schema)}.ducklake_metadata
            WHERE scope IS NULL
          SQL

          result = execute(sql, 'Get DuckLake Options')
          options = {}
          result.each do |row|
            key, value = row
            # Only include user-configurable options, not internal metadata
            options[key] = value if DUMPABLE_DUCKLAKE_OPTIONS.include?(key)
          end

          options.empty? ? nil : options
        rescue StandardError
          nil
        end

        # Returns DuckLake options for a specific table
        # @param table_name [String, Symbol] The table name
        # @return [Hash] Hash of option_name => value for table-scoped options
        # @return [nil] If not in DuckLake mode or no table options set
        def ducklake_table_options(table_name)
          return nil unless ducklake?

          metadata_schema = ducklake_metadata_schema
          return nil unless metadata_schema

          # Get the table_id first
          table_sql = <<~SQL
            SELECT table_id FROM #{quote_table_name(metadata_schema)}.ducklake_table
            WHERE table_name = #{quote(table_name.to_s)}
          SQL
          table_result = execute(table_sql, 'Get Table ID')
          table_id = table_result.first&.first
          return nil unless table_id

          # Query the ducklake_metadata table for table-scoped options
          sql = <<~SQL
            SELECT key, value
            FROM #{quote_table_name(metadata_schema)}.ducklake_metadata
            WHERE scope = 'table' AND scope_id = #{table_id.to_i}
          SQL

          result = execute(sql, 'Get DuckLake Table Options')
          options = result.to_h { |key, value| DUMPABLE_DUCKLAKE_OPTIONS.include?(key) ? [key, value] : [] }

          options.presence
        rescue StandardError
          nil
        end

        # Returns indexes for a specific table
        # @param table_name [String, Symbol] The name of the table
        # @return [Array<ActiveRecord::ConnectionAdapters::IndexDefinition>] Array of index definitions
        def indexes(table_name)
          indexes = []
          begin
            result = execute("SELECT * FROM duckdb_indexes() WHERE table_name = #{quote(table_name.to_s)}", 'SCHEMA')
            # Store result as array immediately to avoid consumption issues
            result_array = result.to_a
            result_array.each_with_index do |index_row, _idx|
              # DuckDB duckdb_indexes() returns array with structure:
              # [database_name, database_oid, schema_name, schema_oid, index_name, index_oid, table_name, table_oid, nil, {}, is_unique, is_primary, column_names, sql]
              index_name = index_row[4]
              is_unique = index_row[10]
              is_primary = index_row[11]
              column_names_str = index_row[12]
              # Skip primary key indexes as they're handled separately
              next if is_primary

              # Skip if we don't have essential information
              next unless index_name && column_names_str

              # Parse column names from string format like "[name]" or "['name']"
              columns = parse_index_columns(column_names_str)
              next if columns.empty?

              # Clean up column names - remove extra quotes
              cleaned_columns = columns.map { |col| col.gsub(/^"|"$/, '') }

              # Create IndexDefinition with correct Rails 8.0 signature
              index_def = ActiveRecord::ConnectionAdapters::IndexDefinition.new(
                table_name.to_s,      # table
                index_name.to_s,      # name
                !is_unique.nil?, # unique
                cleaned_columns # columns
              )
              indexes << index_def
            end
          rescue StandardError => e
            Rails.logger&.warn("Could not retrieve indexes for table #{table_name}: #{e.message}") if defined?(Rails)
          end
          indexes
        end

        # Generates SQL for querying data sources (tables/views) with optional filtering
        # @param name [String, nil] Optional table name to filter by
        # @param type [String, nil] Optional table type filter ('BASE TABLE', 'VIEW', etc.)
        # @return [String] SQL query string for retrieving table information
        def data_source_sql(name = nil, type: nil)
          scope = quoted_scope(name, type: type)

          sql = 'SELECT table_name FROM information_schema.tables'

          conditions = []
          conditions << "table_schema = #{scope[:schema]}" if scope[:schema]
          conditions << "table_name = #{scope[:name]}" if scope[:name]
          conditions << scope[:type] if scope[:type] # This now contains the full condition

          sql += " WHERE #{conditions.join(" AND ")}" if conditions.any?
          sql += ' ORDER BY table_name'
          sql
        end

        # Looks up the appropriate cast type for a column based on SQL type metadata
        # @param sql_type_metadata [ActiveRecord::ConnectionAdapters::SqlTypeMetadata] The SQL type metadata
        # @return [ActiveRecord::Type::Value] The appropriate cast type
        def lookup_cast_type_from_column(sql_type_metadata)
          lookup_cast_type(sql_type_metadata.sql_type)
        end

        # Creates a quoted scope hash for table/schema queries with type filtering
        # @param name [String, nil] Optional table name (may include schema prefix)
        # @param type [String, nil] Optional table type filter
        # @return [Hash] Hash containing quoted schema, name, and type condition
        def quoted_scope(name = nil, type: nil)
          schema, name = extract_schema_qualified_name(name)

          # Default to 'main' schema if no schema specified to avoid returning
          # tables from information_schema, pg_catalog, or other attached databases
          schema ||= 'main'

          type_condition = case type
                           when 'BASE TABLE'
                             "table_type = 'BASE TABLE'"
                           when 'VIEW'
                             "table_type = 'VIEW'"
                           else
                             "table_type IN ('BASE TABLE', 'VIEW')"
                           end

          {
            schema: quote(schema),
            name: name ? quote(name) : nil,
            type: type_condition
          }
        end

        # Converts ActiveRecord type to DuckDB SQL type string
        # @param type [Symbol, String] The ActiveRecord type to convert
        # @param limit [Integer, nil] Optional column size limit
        # @param precision [Integer, nil] Optional decimal precision
        # @param scale [Integer, nil] Optional decimal scale
        # @param options [Hash] Additional type options
        # @return [String] The DuckDB SQL type string
        # @see https://duckdb.org/docs/stable/sql/data_types/overview.html
        def type_to_sql(type, limit: nil, precision: nil, scale: nil, **options)
          case type.to_s
          when 'primary_key'
            # Use the configured primary key type
            primary_key_type_definition
          when 'string', 'text'
            if limit
              "VARCHAR(#{limit})"
            else
              'VARCHAR'
            end
          when 'integer'
            integer_to_sql(limit)
          when 'bigint'
            'BIGINT'
          when 'float', 'real'
            'REAL'
          when 'double'
            'DOUBLE'
          when 'decimal', 'numeric'
            if precision && scale
              "DECIMAL(#{precision},#{scale})"
            elsif precision
              "DECIMAL(#{precision})"
            else
              'DECIMAL'
            end
          when 'datetime', 'timestamp'
            'TIMESTAMP'
          when 'time'
            'TIME'
          when 'date'
            'DATE'
          when 'boolean'
            'BOOLEAN'
          when 'binary', 'blob'
            # TODO: Add blob size limits
            # Postgres has limits set on blob sized
            # https://github.com/rails/rails/blob/82e9029bbf63a33b69f007927979c5564a6afe9e/activerecord/lib/active_record/connection_adapters/postgresql/schema_statements.rb#L855
            # Duckdb has a 4g size limit as well - https://duckdb.org/docs/stable/sql/data_types/blob
            'BLOB'
          when 'uuid'
            'UUID'
          # DuckDB-specific signed integer types
          when 'tinyint'
            'TINYINT'
          when 'smallint'
            'SMALLINT'
          when 'hugeint'
            'HUGEINT'
          # DuckDB-specific unsigned integer types
          when 'utinyint'
            'UTINYINT'
          when 'usmallint'
            'USMALLINT'
          when 'uinteger'
            'UINTEGER'
          when 'ubigint'
            'UBIGINT'
          when 'uhugeint'
            'UHUGEINT'
          # DuckDB interval type for time periods
          when 'interval'
            'INTERVAL'
          else
            super
          end
        end

        # Override execute to intercept CREATE TABLE statements and inject sequence defaults
        # @param sql [String] The SQL statement to execute
        # @param name [String, nil] Optional name for logging purposes
        # @return [DuckDB::Result] The result of the query execution
        def execute(sql, name = nil)
          # Check if this is a CREATE TABLE statement and we have a pending sequence default
          if @pending_sequence_default && sql.match?(/\A\s*CREATE TABLE/i)
            pending = @pending_sequence_default
            table_pattern = /CREATE TABLE\s+"?#{Regexp.escape(pending[:table])}"?\s*\(/i

            if sql.match?(table_pattern)
              # Find the PRIMARY KEY column definition and inject the sequence default
              # This pattern specifically looks for the primary key column with PRIMARY KEY constraint
              pk_column_pattern = /"?#{Regexp.escape(pending[:column])}"?\s+\w+\s+PRIMARY\s+KEY(?!\s+DEFAULT)/i

              # Only replace the first occurrence (the actual primary key)
              sql = sql.sub(pk_column_pattern) do |match|
                # Inject the sequence default before PRIMARY KEY
                match.sub(/(\s+)PRIMARY\s+KEY/i, "\\1DEFAULT nextval(#{quote(pending[:sequence])}) PRIMARY KEY")
              end
            end
          end

          super
        end

        private

        # Parses DuckDB field information and returns a hash with all values needed
        # to construct a Column object. Used by version-specific new_column_from_field.
        # @param table_name [String] The name of the table
        # @param field [Array] Array from PRAGMA table_info
        # @return [Hash] Parsed column information
        def column_info_from_field(table_name, field)
          column_name, formatted_type, column_default, not_null, _type_id, _type_modifier,
            collation_name, comment, _identity, _generated, pk = field

          # Normalize values
          column_name = column_name.presence || 'unknown_column'
          formatted_type = formatted_type.to_s.presence || 'VARCHAR'
          sql_type_metadata = fetch_type_metadata(formatted_type)

          # Determine default value vs function
          default_value, default_function = parse_column_default(
            table_name, column_name, column_default, formatted_type, pk
          )

          # Parse null constraint (DuckDB: not_null=1 means NOT NULL)
          is_null = !not_null.in?([1, true])

          # Detect auto-increment columns
          is_integer_pk = pk && formatted_type.upcase.in?(%w[INTEGER BIGINT])

          {
            name: column_name.to_s,
            sql_type_metadata: sql_type_metadata,
            default: default_value,
            default_function: default_function&.to_s,
            null: is_null,
            collation: collation_name.to_s.presence,
            comment: comment.to_s.presence,
            auto_increment: is_integer_pk,
            rowid: pk && column_name == 'id'
          }
        end

        # Parses column default, separating static values from sequence functions.
        # @return [Array<Object, String>] [default_value, default_function]
        def parse_column_default(table_name, column_name, column_default, formatted_type, pk)
          # Integer primary key named 'id' - assume sequence exists
          if pk && pk.in?([true, 1]) && formatted_type.upcase.in?(%w[INTEGER BIGINT]) && column_name == 'id'
            # Build the sequence name and quote it properly for the nextval expression
            seq_name = "#{table_name}_#{column_name}_seq"
            [nil, "nextval(#{quote(seq_name)})"]
          elsif column_default.to_s.include?('nextval(')
            [nil, column_default.to_s]
          else
            [extract_value_from_default(column_default), nil]
          end
        end

        # Parses index column names from DuckDB's string representation
        # @param column_names_str [String] The string representation of column names
        # @return [Array<String>] Array of cleaned column names
        def parse_index_columns(column_names_str)
          columns = []
          if column_names_str.is_a?(String)
            # Remove outer brackets and split on comma
            cleaned = column_names_str.gsub(/^\[|\]$/, '')
            columns = if cleaned.include?(',')
                        # Multiple columns - split and clean each
                        cleaned.split(',').map { |col| col.strip.gsub(/^['"]|['"]$/, '') }
                      else
                        # Single column - just clean it
                        [cleaned.gsub(/^['"]|['"]$/, '')]
                      end
          end
          columns
        end

        # Safely creates a sequence with proper error handling
        # @param sequence_name [String] The name of the sequence to create
        # @param table_name [String] The table name for logging purposes
        # @param start_with [Integer] The starting value for the sequence (default: 1)
        # @return [void]
        def create_sequence_safely(sequence_name, table_name, start_with: 1)
          return if sequence_exists?(sequence_name)

          begin
            create_sequence(sequence_name, start_with: start_with)
            Rails.logger&.debug("Created sequence #{sequence_name} for table #{table_name} starting at #{start_with}") if defined?(Rails)
          rescue ActiveRecord::StatementInvalid, DuckDB::Error => e
            if e.message.include?('already exists') || e.message.include?('Object already exists')
              # Sequence already exists, which is fine
              Rails.logger&.debug("Sequence #{sequence_name} already exists") if defined?(Rails)
            elsif defined?(Rails)
              # Log the error but don't fail the migration
              Rails.logger&.warn("Could not create sequence #{sequence_name} for table #{table_name}: #{e.message}") if defined?(Rails)
            end
          rescue StandardError => e
            # Catch any other errors and log them, but don't fail the migration
            Rails.logger&.warn("Unexpected error creating sequence #{sequence_name}: #{e.message}") if defined?(Rails)
          end
        end

        # Fetches type metadata by parsing DuckDB SQL type strings
        # @param sql_type [String] The SQL type string to parse
        # @return [ActiveRecord::ConnectionAdapters::SqlTypeMetadata] The parsed type metadata
        def fetch_type_metadata(sql_type)
          # Parse DuckDB types and map to Rails types
          sql_type_str = sql_type.to_s
          type, limit, precision, scale = parse_type_info(sql_type_str)

          # Ensure all parameters are properly set with defaults
          type = (type || :string).to_sym
          limit = nil unless limit&.positive?
          precision = nil unless precision&.positive?
          scale = nil unless scale&.>=(0)

          ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
            sql_type: sql_type_str,
            type: type.to_sym,
            limit: limit,
            precision: precision,
            scale: scale
          )
        end

        # Extracts and converts default values from DuckDB column defaults
        # @param default [String, nil] The default value from column definition
        # @return [Object, nil] The converted default value
        def extract_value_from_default(default)
          return nil if default.nil?

          # IMPORTANT: Return nil for sequence defaults so Rails doesn't set id=0
          return nil if default.to_s.include?('nextval(')

          # Handle DuckDB default value formats
          default_str = default&.to_s&.strip

          case default_str
          when /^'(.*)'$/
            # Remove outer quotes from string defaults
            ::Regexp.last_match(1)
          when 'NULL', ''
            nil
          when /^CAST\('([tf])' AS BOOLEAN\)$/i
            # Handle DuckDB boolean format: CAST('t' AS BOOLEAN) or CAST('f' AS BOOLEAN)
            # Return string representation to avoid ActiveRecord deduplication issues
            ::Regexp.last_match(1).downcase == 't' ? 'true' : 'false'
          when /^-?\d+$/, /^\d+$/
            # Integer defaults (handle both positive and negative)
            default_str.to_i
          when /^-?\d+\.\d+$/, /^\d+\.\d+$/
            # Float defaults (handle both positive and negative)
            # Positive float defaults
            default_str.to_f
          else
            # For any other format, return the string as-is
            default_str
          end
        end

        # Parses DuckDB SQL type strings into Rails type components
        # @param sql_type [String] The SQL type string to parse
        # @return [Array] Array containing [type, limit, precision, scale]
        # @see https://duckdb.org/docs/stable/sql/data_types/overview.html
        def parse_type_info(sql_type)
          return [:string, nil, nil, nil] if sql_type.nil? || sql_type.empty?

          case sql_type&.to_s&.upcase
          when /^INTEGER(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:integer, 4, precision, scale]
          # BIGINT: 8 bytes (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
          when /^BIGINT/i
            [:bigint, 8, nil, nil]
          # HUGEINT: 16 bytes - map to bigint with limit 8 for Rails compatibility
          # Note: Rails doesn't support 16-byte integers natively, values may overflow
          when /^HUGEINT/i
            [:bigint, 8, nil, nil]
          # DuckDB-specific signed integer types - use specific type symbols for schema dumping
          when /^SMALLINT/i
            [:smallint, 2, nil, nil]
          when /^TINYINT/i
            [:tinyint, 1, nil, nil]
          # DuckDB-specific unsigned integer types - use specific type symbols for schema dumping
          when /^UBIGINT/i
            [:ubigint, 8, nil, nil]
          when /^UHUGEINT/i
            [:uhugeint, nil, nil, nil]
          when /^UINTEGER/i
            [:uinteger, 4, nil, nil]
          when /^USMALLINT/i
            [:usmallint, 2, nil, nil]
          when /^UTINYINT/i
            [:utinyint, 1, nil, nil]
          when /^VARCHAR(\((\d+)\))?/i, /^CHAR(\((\d+)\))?/i
            # Extract limit from VARCHAR(n) format
            limit = ::Regexp.last_match(2)&.to_i
            [:string, limit, nil, nil]
          when /^TEXT/i
            [:text, nil, nil, nil]
          when /^JSON/i
            [:json, nil, nil, nil]
          when /^DOUBLE/i, /^REAL/i, /^FLOAT/i
            [:float, nil, nil, nil]
          when /^BOOLEAN/i, /^BOOL/i, /^LOGICAL/i
            [:boolean, nil, nil, nil]
          when /^DATE$/i
            [:date, nil, nil, nil]
          when /^TIMESTAMP/i, /^DATETIME/i
            [:datetime, nil, nil, nil]
          when /^TIME$/i
            [:time, nil, nil, nil]
          when /^DECIMAL(\((\d+),(\d+)\))?/i, /^NUMERIC(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:decimal, nil, precision, scale]
          when /^UUID/i
            [:uuid, nil, nil, nil]
          when /^INTERVAL/i
            [:interval, nil, nil, nil]
          when /^BLOB/i, /^BYTEA/i, /^BINARY/i
            [:binary, nil, nil, nil]
          else
            [:string, nil, nil, nil] # Default fallback
          end
        end

        # Generates a default sequence name for a table and column
        # @param table_name [String] The name of the table
        # @param column_name [String] The name of the column (default: 'id')
        # @return [String] The generated sequence name
        def default_sequence_name(table_name, column_name = 'id')
          "#{table_name}_#{column_name}_seq"
        end

        # Gets the default integer type for primary keys
        # @return [Symbol] The primary key type (:integer or :bigint)
        def integer_primary_key_type
          case self.class.primary_key_type
          when :integer
            :integer
          else
            :bigint # Default to bigint for modern Rails apps
          end
        end

        # Indicates whether DuckDB supports sequences
        # @return [Boolean] always returns true
        def supports_sequences?
          true
        end

        # Extracts schema and table name from a qualified name string
        # @param string [String, nil] The qualified name string to parse
        # @return [Array] Array containing [schema, name]
        def extract_schema_qualified_name(string)
          return [nil, nil] if string.nil?

          schema, name = string.to_s.scan(/[^".\s]+|"[^"]*"/)[0, 2]
          schema, name = nil, schema unless name
          [schema, name]
        end

        # Returns the default primary key type definition for DuckDB
        # DuckLake doesn't support PRIMARY KEY constraints, so we omit them in that mode
        # @return [String] SQL definition for the primary key type
        def primary_key_type_definition
          base_type = case self.class.primary_key_type
                      when :uuid then 'UUID'
                      when :bigint then 'BIGINT'
                      when :string then 'VARCHAR'
                      else 'INTEGER'
                      end

          # DuckLake doesn't support PRIMARY KEY/UNIQUE constraints
          ducklake? ? base_type : "#{base_type} PRIMARY KEY"
        end

        # Converts integer limit to appropriate DuckDB integer SQL type
        # @param limit [Integer, nil] The byte limit for the integer type
        # @return [String] The DuckDB SQL integer type
        # @raise [ArgumentError] if limit is not supported
        # @see https://duckdb.org/docs/stable/sql/data_types/numeric
        def integer_to_sql(limit)
          case limit
          when 1
            'TINYINT'   # 1 byte: -128 to 127
          when 2
            'SMALLINT'  # 2 bytes: -32,768 to 32,767
          when nil, 3, 4
            'INTEGER'   # 4 bytes: -2,147,483,648 to 2,147,483,647
          when 5..8
            'BIGINT'    # 8 bytes: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
          when 9..16
            'HUGEINT'   # 16 bytes: -2^127 to 2^127-1
          else
            raise ArgumentError, "No integer type has byte size #{limit}. Use a decimal with scale 0 instead."
          end
        end
      end
    end
  end
end
