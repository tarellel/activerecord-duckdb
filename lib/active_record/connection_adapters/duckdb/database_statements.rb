# frozen_string_literal: true

# This module provides database statements for the DuckDB adapter.
module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatements
        # SQL statements that are considered read-only (SELECT, EXPLAIN, etc.)
        READ_QUERY_PATTERN = /\A\s*(SELECT|SHOW|DESCRIBE|EXPLAIN|PRAGMA)\b/i

        # Begins a database transaction.
        # @return [void]
        def begin_db_transaction
          log('BEGIN', 'TRANSACTION') do
            with_raw_connection do |conn|
              conn.query('BEGIN TRANSACTION')
            end
          end
        end

        # Commits the current database transaction.
        # @return [void]
        def commit_db_transaction
          log('COMMIT', 'TRANSACTION') do
            with_raw_connection do |conn|
              conn.query('COMMIT')
            end
          end
        end

        # Rolls back the current database transaction.
        # @return [void]
        def exec_rollback_db_transaction
          log('ROLLBACK', 'TRANSACTION') do
            with_raw_connection do |conn|
              conn.query('ROLLBACK')
            end
          end
        end

        # Determines if a SQL query is a write operation (INSERT, UPDATE, DELETE, etc.)
        # Used for read replica support and transaction tracking.
        # @param sql [String] The SQL query to check
        # @return [Boolean] true if the query modifies data
        def write_query?(sql)
          !READ_QUERY_PATTERN.match?(sql)
        end

        # Executes a SQL statement against the DuckDB database.
        # Used for DDL and raw SQL execution.
        # @param sql [String] The SQL statement to execute
        # @param name [String, nil] Optional name for logging purposes
        # @return [DuckDB::Result] The result of the query execution
        def execute(sql, name = nil) # :nodoc:
          # Check for write queries on read-only connections (replica support)
          # Rails 8.1+ uses ensure_writes_are_allowed, earlier versions use check_if_write_query
          ensure_write_query_allowed(sql)

          log(sql, name) do
            with_raw_connection do |conn|
              conn.query(sql)
            end
          end
        end

        # Casts a DuckDB result to ActiveRecord::Result format.
        # Used by Rails 8.0+ internal_exec_query which calls cast_result(raw_execute(...)).
        # Also called by Rails 7.2's internal_exec_query implementation in DatabaseStatementsRails72.
        # @param result [DuckDB::Result, nil] The DuckDB result to cast
        # @return [ActiveRecord::Result] The ActiveRecord-compatible result
        def cast_result(result)
          return ActiveRecord::Result.empty if result.nil?

          columns = result.columns.map do |col|
            if col.respond_to?(:name)
              col.name
            elsif col.respond_to?(:column_name)
              col.column_name
            else
              col.to_s
            end
          end

          ActiveRecord::Result.new(columns, result.to_a)
        end

        # Returns the number of affected rows from a raw DuckDB result.
        # Required by Rails 8.0+ for exec_delete/exec_update via internal_execute.
        # The base class calls affected_rows(raw_execute(...)) for DELETE/UPDATE operations.
        # @param raw_result [DuckDB::Result] The raw DuckDB result
        # @return [Integer] Number of rows affected
        def affected_rows(raw_result)
          raw_result.rows_changed
        end

        # Returns columns that should be included in INSERT statements
        # @param table_name [String] The name of the table
        # @return [Array<ActiveRecord::ConnectionAdapters::Column>] Columns to include in INSERT
        def columns_for_insert(table_name)
          columns(table_name).reject do |column|
            # Exclude columns that have a default function (like nextval)
            column.default_function.present?
          end
        end

        # Determines if a column value should be returned after insert
        # This is crucial - it tells Rails which columns should use RETURNING
        # @param column [ActiveRecord::ConnectionAdapters::Column] The column to check
        # @return [Boolean] true if column value should be returned after insert
        def return_value_after_insert?(column)
          # Return true for any column with a sequence default
          column.default_function&.include?('nextval') || super
        end

        # Extracts the last inserted ID from an insert result
        # @param result [ActiveRecord::Result] The result from an insert operation
        # @return [Object] The last inserted ID value
        def last_inserted_id(result)
          # Handle ActiveRecord::Result from RETURNING clause
          if result.is_a?(ActiveRecord::Result) && result.rows.any?
            id_value = result.rows.first.first
            return id_value
          end
          super
        end

        private

        # Ensures write queries are allowed on the current connection.
        # Handles API differences between Rails versions:
        # - Rails 8.1+: Uses ensure_writes_are_allowed
        # - Rails 7.2-8.0: Uses check_if_write_query + mark_transaction_written_if_write
        # @param sql [String] The SQL query to check
        def ensure_write_query_allowed(sql)
          if respond_to?(:ensure_writes_are_allowed, true)
            # Rails 8.1+
            ensure_writes_are_allowed(sql)
          else
            # Rails 7.2-8.0
            check_if_write_query(sql)
            mark_transaction_written_if_write(sql)
          end
        end

        # Extracts and converts default values from DuckDB column defaults
        # @param default [String, nil] The default value from column definition
        # @return [Object, nil] The converted default value
        def extract_value_from_default(default)
          return nil if default.nil?

          # IMPORTANT: Return nil for sequence defaults so Rails doesn't set id=0
          return nil if default.to_s.include?('nextval(')

          # Handle DuckDB default value formats
          case default.to_s
          when /^'(.*)'$/
            ::Regexp.last_match(1) # Remove quotes from string defaults
          when 'NULL'
            nil
          when /^\d+$/
            default.to_i # Integer defaults
          when /^\d+\.\d+$/
            default.to_f # Float defaults
          else
            default
          end
        end
      end
    end
  end
end
