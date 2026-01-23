# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific schema dumper class for generating schema.rb files
      # Extends Rails' ConnectionAdapters::SchemaDumper to handle DuckDB-specific
      # column types, constraints, and DuckLake features like partitioning
      class SchemaDumper < ConnectionAdapters::SchemaDumper # :nodoc:
        # Override table dumping to include DuckLake-specific features
        # @param table [String] The table name
        # @param stream [IO] The output stream
        # @return [void]
        def table(table, stream)
          # Call the parent implementation first
          super

          # Add DuckLake partitioning if present
          dump_partitioning(table, stream)

          # Add DuckLake table-level options if present
          dump_table_options(table, stream)
        end

        private

        # Override Scenic's defined_views to return empty array for DuckDB
        # This prevents Scenic's MySQL adapter from being called with DuckDB connections
        # @return [Array] Empty array of views
        def defined_views
          []
        end

        # Override Scenic's dumpable_views_in_database to return empty array for DuckDB
        # This prevents Scenic from calling Scenic.database.views which uses MySQL adapter
        # @return [Array] Empty array of views
        def dumpable_views_in_database
          []
        end

        # Override extensions to also dump DuckLake options
        # @param stream [IO] The output stream
        # @return [void]
        def extensions(stream)
          super
          dump_ducklake_options(stream)
        end

        # Valid DuckLake options that can be set via set_option
        SETTABLE_DUCKLAKE_OPTIONS = %w[parquet_version parquet_compression].freeze

        # Dumps DuckLake options (parquet_version, parquet_compression, etc.)
        # @param stream [IO] The output stream
        # @return [void]
        def dump_ducklake_options(stream)
          return unless @connection.respond_to?(:ducklake_options)

          options = @connection.ducklake_options
          return if options.nil? || options.empty?

          # Filter to only include settable options (not metadata like 'encrypted')
          settable_options = options.select { |name, _| SETTABLE_DUCKLAKE_OPTIONS.include?(name) }
          return if settable_options.empty?

          settable_options.sort.each do |name, value|
            stream.puts "  set_ducklake_option #{name.inspect}, #{value.inspect}"
          end
          stream.puts
        end

        # Dumps DuckLake partitioning for a table
        # @param table_name [String] The table name
        # @param stream [IO] The output stream
        # @return [void]
        def dump_partitioning(table_name, stream)
          return unless @connection.respond_to?(:partition_expressions)

          expressions = @connection.partition_expressions(table_name)
          return if expressions.nil? || expressions.empty?

          # Format the expressions array as Ruby code
          expressions_code = expressions.map { |e| e.inspect }.join(', ')
          stream.puts "  set_partitioned_by #{table_name.inspect}, [#{expressions_code}]"
        end

        # Dumps DuckLake table-level options
        # @param table_name [String] The table name
        # @param stream [IO] The output stream
        # @return [void]
        def dump_table_options(table_name, stream)
          return unless @connection.respond_to?(:ducklake_table_options)

          options = @connection.ducklake_table_options(table_name)
          return if options.nil? || options.empty?

          options.sort.each do |name, value|
            stream.puts "  set_ducklake_option #{name.inspect}, #{value.inspect}, #{table_name.inspect}"
          end
          stream.puts
        end

        # Generates column specification for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to generate spec for
        # @return [Array] Array containing column type and options hash
        def column_spec(column)
          column_type = schema_type(column)
          column_options = prepare_column_options(column)
          # For any column with sequence defaults, include them in column options
          if column.respond_to?(:default_function) && column.default_function&.include?('nextval(')
            # Include the sequence function as column default
            column_options[:default] = "-> { \"#{column.default_function}\" }"
          end
          [column_type, column_options]
        end

        # Maps DuckDB SQL types to ActiveRecord schema types
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to map
        # @return [Symbol] The ActiveRecord schema type symbol
        def schema_type(column)
          case column.sql_type.to_s.upcase
          when /^BIGINT$/i
            :bigint
          when /^INTEGER$/i
            :integer
          when /^VARCHAR$/i, /^VARCHAR\(\d+\)$/i
            :string
          when /^TEXT$/i
            :text
          when /^TIMESTAMP$/i
            :datetime
          when /^BOOLEAN$/i
            :boolean
          when /^UUID$/i
            :uuid
          when /^DECIMAL\((\d+),(\d+)\)$/i
            :decimal
          when /^BLOB$/i
            :binary
          when /^REAL$/i, /^DOUBLE$/i
            :float
          when /^DATE$/i
            :date
          when /^TIME$/i
            :time
          # DuckDB-specific signed integer types
          when /^TINYINT$/i
            :tinyint
          when /^SMALLINT$/i
            :smallint
          when /^HUGEINT$/i
            :hugeint
          # DuckDB-specific unsigned integer types
          when /^UTINYINT$/i
            :utinyint
          when /^USMALLINT$/i
            :usmallint
          when /^UINTEGER$/i
            :uinteger
          when /^UBIGINT$/i
            :ubigint
          when /^UHUGEINT$/i
            :uhugeint
          # DuckDB interval type
          when /^INTERVAL$/i
            :interval
          else
            column.type
          end
        end

        # Determines if a column uses the default primary key behavior
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to check
        # @return [Boolean] true if column uses default primary key behavior
        def default_primary_key?(column)
          # Never treat sequence-based primary keys as having default behavior
          return false if column.respond_to?(:default_function) && column.default_function&.include?('nextval(')

          # Only consider it a default primary key if it's bigint without sequences
          schema_type(column) == :bigint
        end

        # Determines if a primary key column requires explicit default inclusion
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to check
        # @return [Boolean] true if column requires explicit default in schema
        def explicit_primary_key_default?(column)
          # Return true for any column with sequence defaults to force explicit inclusion
          column.respond_to?(:default_function) && column.default_function&.include?('nextval(')
        end

        # Prepares column options hash for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to prepare options for
        # @return [Hash] Hash of column options for schema dumping
        def prepare_column_options(column)
          spec = {}

          # Add limit only for string types and when meaningful
          if (limit = schema_limit(column))
            spec[:limit] = limit
          end

          # Add precision only for numeric types and when meaningful (not nil/zero)
          if (precision = schema_precision(column))
            spec[:precision] = precision
          end

          # Add scale only for numeric types and when meaningful
          if (scale = schema_scale(column))
            spec[:scale] = scale
          end

          # Add null constraint
          spec[:null] = false unless column.null

          # Add default value if present and not a function (sequences handled in column_spec)
          if (default = schema_default(column))
            spec[:default] = default
          end

          # Add comment if present
          spec[:comment] = column.comment.inspect if column.comment.present?
          spec = spec.compact
          spec
        end

        # Extracts limit option for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to extract limit from
        # @return [Integer, nil] The column limit or nil if not applicable
        def schema_limit(column)
          return column.limit if column.limit && column.type == :string

          nil
        end

        # Extracts precision option for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to extract precision from
        # @return [Integer, nil] The column precision or nil if not applicable
        def schema_precision(column)
          return nil unless %i[decimal float numeric real].include?(column.type)
          return nil unless column.precision&.positive?

          column.precision
        end

        # Extracts scale option for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to extract scale from
        # @return [Integer, nil] The column scale or nil if not applicable
        def schema_scale(column)
          return nil unless %i[decimal float numeric real].include?(column.type)
          return nil unless column.scale && column.scale >= 0

          column.scale
        end

        # Extracts and formats default value for schema dumping
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to extract default from
        # @return [Object, nil] The formatted default value or nil if no default
        def schema_default(column)
          return nil if column.respond_to?(:default_function) && column.default_function

          case column.default
          when nil
            nil
          when true, false, Numeric
            column.default
          when String
            # Handle DuckDB's boolean format: CAST('t' AS BOOLEAN) or CAST('f' AS BOOLEAN)
            if column.default.match?(/^CAST\('([tf])' AS BOOLEAN\)$/i)
              column.default.match(/^CAST\('([tf])' AS BOOLEAN\)$/i)[1].downcase == 't'
            else
              column.default.inspect
            end
          else
            column.default.inspect
          end
        end
      end
    end
  end
end
