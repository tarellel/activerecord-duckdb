# frozen_string_literal: true

# Rails 8.0+ specific database statements.
# In Rails 8.0+, the base class internal_exec_query delegates to raw_execute + cast_result,
# so we implement raw_execute and let Rails handle the rest.
# This allows Rails' built-in query execution infrastructure (logging, retries, etc.) to work.
module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatementsRails8
        # Executes raw SQL and returns the native DuckDB result.
        # Called by Rails 8's internal_execute → raw_execute chain.
        #
        # @param sql [String] The SQL query to execute
        # @param name [String] Query name for logging
        # @param binds [Array] Array of bind parameters
        # @param prepare [Boolean] Whether to use prepared statement (unused by DuckDB)
        # @param async [Boolean] Whether to execute asynchronously (unused by DuckDB)
        # @param allow_retry [Boolean] Whether to allow retrying on failure
        # @param materialize_transactions [Boolean] Whether to materialize transactions
        # @param batch [Boolean] Whether this is a batch operation (unused by DuckDB)
        # @return [DuckDB::Result] The raw DuckDB result
        def raw_execute(sql, name = nil, binds = [], prepare: false, async: false, allow_retry: false, materialize_transactions: true, batch: false)
          casted_binds = type_casted_binds(binds)

          log(sql, name, binds, casted_binds, async: async) do
            with_raw_connection(allow_retry:, materialize_transactions:) do |conn|
              if casted_binds.empty?
                conn.query(sql)
              else
                conn.query(sql, *casted_binds)
              end
            end
          end
        end
      end
    end
  end
end
