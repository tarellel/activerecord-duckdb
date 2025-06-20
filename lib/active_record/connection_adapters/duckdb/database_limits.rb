# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    # DuckDB Limits - https://duckdb.org/docs/stable/operations_manual/limits.html
    module Duckdb
      module DatabaseLimits
        # Since DuckDb is PostgreSQL compatible (64-1), we can use the same limits as PostgreSQL.
        # In the future want to drop possible PostgreSQL compatibility
        # we can change the limits to match whatever limits DuckDB may want to impose
        # https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        # Returns the maximum length for database identifiers
        # @return [Integer] The maximum identifier length (63 characters for PostgreSQL compatibility)
        def max_identifier_length
          63
        end

        # Returns the maximum length for table aliases
        # @return [Integer] The maximum table alias length (delegates to max_identifier_length)
        def table_alias_length
          max_identifier_length
        end

        # Returns the maximum length for table names
        # @return [Integer] The maximum table name length (delegates to max_identifier_length)
        def table_name_length
          max_identifier_length
        end

        # Returns the maximum length for index names
        # @return [Integer] The maximum index name length (delegates to max_identifier_length)
        def index_name_length
          max_identifier_length
        end

        private

        # The max number of binds is 1024
        # Returns the maximum number of bind parameters for queries
        # @return [Integer] The maximum number of bind parameters (1000)
        def bind_params_length
          1_000
        end

        # https://duckdb.org/docs/stable/data/insert.html
        # Returns the maximum number of rows that can be inserted in a single operation
        # @return [Integer] The maximum number of rows for batch inserts (1000)
        def insert_rows_length
          1_000
        end
      end
    end
  end
end
