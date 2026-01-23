# frozen_string_literal: true

# Rails 7.2 specific database statements.
# In Rails 7.2, the base class internal_exec_query raises NotImplementedError,
# so we must provide a complete implementation.
module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatementsRails72
        # Executes a query and returns an ActiveRecord::Result.
        # Required for Rails 7.2 where the base class raises NotImplementedError.
        #
        # @param sql [String] The SQL query to execute
        # @param name [String] Query name for logging
        # @param binds [Array] Array of bind parameters
        # @param prepare [Boolean] Whether to prepare the statement (unused - DuckDB doesn't cache)
        # @param async [Boolean] Whether to execute asynchronously (unused)
        # @param allow_retry [Boolean] Whether to allow retrying on failure
        # @return [ActiveRecord::Result] The query result
        def internal_exec_query(sql, name = 'SQL', binds = [], prepare: false, async: false, allow_retry: false)
          # Check for write queries on read-only connections (replica support)
          ensure_write_query_allowed(sql)

          casted_binds = type_casted_binds(binds)

          log(sql, name, binds, casted_binds, async:) do
            with_raw_connection(allow_retry:) do |conn|
              result = if casted_binds.empty?
                         conn.query(sql)
                       else
                         conn.query(sql, *casted_binds)
                       end
              cast_result(result)
            end
          end
        end

        # Executes a DELETE statement and returns the number of affected rows.
        # Rails 7.2's base class returns ActiveRecord::Result, but we need an integer.
        # DuckDB's DELETE returns the count in the result set as rows[0][0].
        # @param sql [String] The DELETE SQL statement
        # @param name [String, nil] Optional name for logging
        # @param binds [Array] Array of bind parameters
        # @return [Integer] Number of rows affected by the delete
        def exec_delete(sql, name = nil, binds = [])
          result = internal_exec_query(sql, name, binds)
          result.rows.first&.first || 0
        end
        alias exec_update exec_delete
      end
    end
  end
end
