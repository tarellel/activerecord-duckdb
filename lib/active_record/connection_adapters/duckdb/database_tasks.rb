# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific database tasks for database lifecycle management
      # Provides stub implementations for database creation and destruction operations
      module DatabaseTasks
        # Creates a DuckDB database (stub implementation)
        # @param database [String] The database name or path
        # @param options [Hash] Additional options for database creation (unused)
        # @return [void]
        def create_database(database, options = {}); end

        # Drops a DuckDB database (stub implementation)
        # @param database [String] The database name or path
        # @return [void]
        def drop_database(database); end
      end
    end
  end
end
