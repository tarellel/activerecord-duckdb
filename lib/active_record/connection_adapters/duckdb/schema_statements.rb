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
          sql << " START #{start_with}" if start_with != 1
          sql << " INCREMENT #{increment_by}" if increment_by != 1
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
          "nextval('#{sequence_name}')"
        end

        # Resets a sequence to a specific value
        # @param sequence_name [String] The name of the sequence to reset
        # @param value [Integer] The value to reset the sequence to (default: 1)
        # @return [void]
        def reset_sequence!(sequence_name, value = 1)
          execute("ALTER SEQUENCE #{quote_table_name(sequence_name)} RESTART WITH #{value}", 'Reset Sequence')
        end

        # Returns a list of all sequences in the database
        # @return [Array<String>] Array of sequence names (currently returns empty array)
        def sequences
          # For now, return empty array since DuckDB sequence introspection is limited
          []
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

        # Creates a new Column object from DuckDB field information
        # @param table_name [String] The name of the table
        # @param field [Array] Array containing column field information from PRAGMA table_info
        # @param definitions [Hash] Additional column definitions (unused)
        # @return [ActiveRecord::ConnectionAdapters::Duckdb::Column] The created column object
        def new_column_from_field(table_name, field, definitions)
          column_name, formatted_type, column_default, not_null, _type_id, _type_modifier, collation_name, comment, _identity, _generated, pk = field

          # Ensure we have required values with proper defaults
          column_name = 'unknown_column' if column_name.nil? || column_name.empty?

          formatted_type = formatted_type.to_s if formatted_type
          formatted_type = 'VARCHAR' if formatted_type.nil? || formatted_type.empty?

          # Create proper SqlTypeMetadata object
          sql_type_metadata = fetch_type_metadata(formatted_type)

          # For primary keys with integer types, check if sequence exists and set default_function
          default_value = nil
          default_function = nil

          if pk && [true,
                    1].include?(pk) && %w[INTEGER BIGINT].include?(formatted_type.to_s.upcase) && column_name == 'id'
            # This is an integer primary key named 'id' - assume sequence exists
            sequence_name = "#{table_name}_#{column_name}_seq"
            default_function = "nextval('#{sequence_name}')"
            default_value = nil
          elsif column_default&.to_s&.include?('nextval(')
            # This is a sequence - store it as default_function, not default_value
            default_function = column_default.to_s
            default_value = nil
          else
            default_value = extract_value_from_default(column_default)
            default_function = nil
          end

          # Ensure boolean values are properly converted for null constraint
          # In DuckDB PRAGMA: not_null=1 means NOT NULL, not_null=0 means NULL allowed
          is_null = case not_null
                    when 1, true
                      false  # Column does NOT allow NULL
                    else
                      true   # Default to allowing NULL for unknown values
                    end

          # Clean up parameters for Column constructor
          clean_column_name = column_name.to_s
          clean_default_value = default_value
          clean_default_function = default_function&.to_s
          clean_collation = collation_name&.to_s
          clean_comment = comment&.to_s

          ActiveRecord::ConnectionAdapters::Duckdb::Column.new(
            clean_column_name,       # name
            clean_default_value,     # default (should be nil for sequences!)
            sql_type_metadata,       # sql_type_metadata
            is_null,                 # null (boolean - true if column allows NULL)
            clean_default_function,  # default_function (this is where nextval goes!)
            collation: clean_collation.presence,
            comment: clean_comment.presence,
            auto_increment: pk && %w[INTEGER BIGINT].include?(formatted_type.to_s.upcase),
            rowid: pk && column_name == 'id'
          )
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

          type_condition = case type
                           when 'BASE TABLE'
                             "table_type = 'BASE TABLE'"
                           when 'VIEW'
                             "table_type = 'VIEW'"
                           else
                             "table_type IN ('BASE TABLE', 'VIEW')"
                           end

          {
            schema: schema ? quote(schema) : nil,
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
          when 'float'
            'REAL'
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
                match.sub(/(\s+)PRIMARY\s+KEY/i, "\\1DEFAULT nextval('#{pending[:sequence]}') PRIMARY KEY")
              end
            end
          end

          super
        end

        private

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
        def parse_type_info(sql_type)
          return [:string, nil, nil, nil] if sql_type.nil? || sql_type.empty?

          # https://duckdb.org/docs/stable/sql/data_types/overview.html
          case sql_type&.to_s&.upcase
          when /^INTEGER(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:integer, nil, precision, scale]
          # Map HUGEINT to bigint for Rails compatibility
          when /^BIGINT/i, /^HUGEINT/i
            [:bigint, nil, nil, nil]
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
          when /^TIME/i
            [:time, nil, nil, nil]
          when /^TIMESTAMP/i, /^DATETIME/i
            [:datetime, nil, nil, nil]
          when /^DECIMAL(\((\d+),(\d+)\))?/i, /^NUMERIC(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:decimal, nil, precision, scale]
          when /^UUID/i
            [:uuid, nil, nil, nil]
          when /^TINYINT/i, /^SMALLINT/i
            # TODO: Determine if integer or smallint should be used here
            [:integer, nil, nil, nil]
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
        # @return [String] SQL definition for the primary key type
        def primary_key_type_definition
          case self.class.primary_key_type
          when :uuid
            'UUID PRIMARY KEY'
          when :bigint
            'BIGINT PRIMARY KEY'
          when :string
            'VARCHAR PRIMARY KEY'
          else
            'INTEGER PRIMARY KEY' # fallback
          end
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
