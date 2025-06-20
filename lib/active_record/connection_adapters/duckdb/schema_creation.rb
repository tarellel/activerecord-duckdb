# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific schema creation functionality
      # Extends Rails' SchemaCreation to handle DuckDB-specific SQL generation
      class SchemaCreation < SchemaCreation
        private

        # Indicates whether DuckDB supports USING clause in index creation
        # No USING clause is supported - DuckDB automatically determines the appropriate index type.
        # https://duckdb.org/docs/stable/sql/statements/create_index.html
        # https://duckdb.org/docs/stable/sql/indexes.html
        # @return [Boolean] always returns false as DuckDB doesn't support USING clause
        def supports_index_using?
          false
        end

        # Adds column options to SQL, with special handling for DuckDB sequence defaults
        # Override to handle nextval() defaults properly for DuckDB sequences
        # @param sql [String] The SQL string being built
        # @param options [Hash] Column options hash
        # @return [void]
        def add_column_options!(sql, options)
          # Handle nextval() function calls - don't quote them
          if options[:default]&.to_s&.include?('nextval(')
            # Extract the sequence name and add it properly
            default_value = options[:default].to_s
            # Remove any extra quotes that might have been added
            default_value = default_value.gsub(/^['"]|['"]$/, '')
            sql << " DEFAULT #{default_value}"
            # Create a copy of options without the default to prevent Rails from processing it again
            options = options.except(:default)
          end

          # Let Rails handle all other column options normally
          super
        end
      end
    end
  end
end
