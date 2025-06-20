# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific schema dumper functionality for generating schema.rb files
      # Provides methods to properly dump DuckDB-specific column types and constraints
      module SchemaDumper
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
