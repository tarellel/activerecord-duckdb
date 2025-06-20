# frozen_string_literal: true

# This module provides database statements for the DuckDB adapter.
module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatements
        # Determines if a SQL query is a write operation
        # @param _sql [String] The SQL query to check (unused in DuckDB implementation)
        # @return [Boolean] always returns false for DuckDB
        def write_query?(_sql)
          false
        end

        # Executes a SQL statement against the DuckDB database
        # @param sql [String] The SQL statement to execute
        # @param name [String, nil] Optional name for logging purposes
        # @return [DuckDB::Result] The result of the query execution
        def execute(sql, name = nil) # :nodoc:
          # In case a query is being executed before the connection is open, reconnect.
          reconnect unless raw_connection

          log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              raw_connection.query(sql)
            end
          end
        end

        # Performs a query with optional bind parameters
        # @param raw_connection [DuckDB::Connection] The raw database connection
        # @param sql [String] The SQL query to execute
        # @param binds [Array] Array of bind parameters
        # @param type_casted_binds [Array] Type-casted bind parameters
        # @param prepare [Boolean] Whether to prepare the statement (unused)
        # @param notification_payload [Hash, nil] Payload for notifications (unused)
        # @param batch [Boolean] Whether this is a batch operation (unused)
        # @param async [Boolean] Whether to execute asynchronously (unused)
        # @param kwargs [Hash] Additional keyword arguments
        # @return [DuckDB::Result] The query result
        def perform_query(raw_connection,
                          sql,
                          binds,
                          type_casted_binds,
                          prepare: false,
                          notification_payload: nil,
                          batch: false,
                          async: false,
                          **kwargs)
          result = if binds.any?
                     # Use the modern parameter binding approach
                     exec_query_with_binds(sql, binds)
                   else
                     # Fallback to direct execution with quoted values
                     # Your existing quote method will handle any unquoted values
                     raw_connection.query(sql)
                   end
          result
        end

        # Casts a DuckDB result to ActiveRecord::Result format
        # @param result [DuckDB::Result, nil] The DuckDB result to cast
        # @return [ActiveRecord::Result] The ActiveRecord-compatible result
        def cast_result(result)
          return ActiveRecord::Result.empty(affected_rows: @affected_rows_before_warnings) if result.nil?

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

        # Executes a query and returns an ActiveRecord::Result
        # @param sql [String] The SQL query to execute
        # @param name [String, nil] Optional name for logging
        # @param binds [Array] Array of bind parameters
        # @param prepare [Boolean] Whether to prepare the statement (unused)
        # @return [ActiveRecord::Result] The query result
        def exec_query(sql, name = nil, binds = [], prepare: false)
          reconnect unless raw_connection

          log(sql, name, binds) do
            result = if binds.any?
                       # Use the modern parameter binding approach
                       exec_query_with_binds(sql, binds)
                     else
                       # Fallback to direct execution with quoted values
                       # Your existing quote method will handle any unquoted values
                       raw_connection.query(sql)
                     end

            build_result(result)
          end
        end

        # Executes a DELETE statement and returns the number of affected rows
        # @param sql [String] The DELETE SQL statement
        # @param name [String, nil] Optional name for logging
        # @param binds [Array] Array of bind parameters
        # @return [Integer] Number of rows affected by the delete
        def exec_delete(sql, name = nil, binds = []) # :nodoc:
          reconnect unless raw_connection

          if binds.any?
            # For bound queries, handle them with proper logging
            log(sql, name, binds) do
              bind_values = binds.map(&:value_for_database)
              raw_connection.query(sql, *bind_values)
            end.rows_changed
          else
            # For non-bound queries, use execute directly
            result = execute(sql, name)
            result.rows_changed
          end
        end
        alias exec_update exec_delete

        # Executes an INSERT statement with optional RETURNING clause
        # @param sql [String] The INSERT SQL statement
        # @param name [String, nil] Optional name for logging
        # @param binds [Array] Array of bind parameters
        # @param pk [String, nil] Primary key column name
        # @param sequence_name [String, nil] Sequence name (unused)
        # @param returning [Array, nil] Columns to return after insert
        # @return [ActiveRecord::Result] The insert result with returned values
        def exec_insert(sql, name = nil, binds = [], pk = nil, sequence_name = nil, returning: nil)
          # Rails 8 will pass returning: [pk] when it wants the ID back
          if returning&.any?
            returning_columns = returning.map { |c| quote_column_name(c) }.join(', ')
            sql = "#{sql} RETURNING #{returning_columns}" unless sql.include?('RETURNING')
          elsif pk && !sql.include?('RETURNING')
            # Add RETURNING for the primary key
            sql = "#{sql} RETURNING #{quote_column_name(pk)}"
          end

          # Execute the query and return the result
          # Rails 8 will handle extracting the ID from the result
          exec_query(sql, name, binds)
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

        # Builds an ActiveRecord::Result from a DuckDB result
        # @param result [DuckDB::Result, nil] The DuckDB result to convert
        # @return [ActiveRecord::Result] The converted ActiveRecord result
        def build_result(result)
          # Handle DuckDB result format
          return ActiveRecord::Result.empty if result.nil?

          # DuckDB results have .columns and .to_a, not .rows
          columns = result.columns.map do |col|
            if col.respond_to?(:name)
              col.name
            elsif col.respond_to?(:column_name)
              col.column_name
            else
              col.to_s
            end
          end

          rows = result.to_a
          ActiveRecord::Result.new(columns, rows)
        end

        # Fetches type metadata for a SQL type string
        # @param sql_type [String] The SQL type to get metadata for
        # @return [ActiveRecord::ConnectionAdapters::SqlTypeMetadata] The type metadata
        def fetch_type_metadata(sql_type)
          # Parse DuckDB types and map to Rails types
          type, limit, precision, scale = parse_type_info(sql_type)

          ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
            sql_type: sql_type,
            type: type.to_sym,
            limit: limit,
            precision: precision,
            scale: scale
          )
        end

        private

        # Executes a query with bind parameters using positional parameters
        # @param sql [String] The SQL query with bind placeholders
        # @param binds [Array] Array of bind parameters
        # @return [DuckDB::Result] The query result
        def exec_query_with_binds(sql, binds)
          # For DuckDB, use positional parameters instead of named parameters
          bind_values = binds.map do |bind|
            if bind.respond_to?(:value_for_database)
              bind.value_for_database
            elsif bind.is_a?(Symbol)
              bind.to_s
            else
              bind
            end
          end
          raw_connection.query(sql, *bind_values)
        end

        # Parses SQL type information to extract Rails type, limit, precision, and scale
        # @param sql_type [String] The SQL type string to parse
        # @return [Array] Array containing [type, limit, precision, scale]
        def parse_type_info(sql_type)
          case sql_type.to_s.upcase
          when /^INTEGER(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:integer, nil, precision, scale]
          when /^VARCHAR(\((\d+)\))?/i, /^TEXT/i
            limit = ::Regexp.last_match(2)&.to_i
            [:string, limit, nil, nil]
          when /^DOUBLE/i, /^REAL/i
            [:float, nil, nil, nil]
          when /^BOOLEAN/i
            [:boolean, nil, nil, nil]
          when /^DATE$/i
            [:date, nil, nil, nil]
          when /^TIMESTAMP/i, /^DATETIME/i
            [:datetime, nil, nil, nil]
          when /^DECIMAL(\((\d+),(\d+)\))?/i, /^NUMERIC(\((\d+),(\d+)\))?/i
            precision, scale = ::Regexp.last_match(2)&.to_i, ::Regexp.last_match(3)&.to_i
            [:decimal, nil, precision, scale]
          else
            [:string, nil, nil, nil] # Default fallback
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

        # Substitutes bind parameters in SQL with quoted values
        # @param sql [String] The SQL string with bind placeholders
        # @param binds [Array] Array of bind parameters
        # @return [String] SQL with substituted values
        def substitute_binds(sql, binds)
          bind_index = 0
          sql.gsub('?') do
            value = quote(binds[bind_index].value)
            bind_index += 1
            value
          end
        end
      end
    end
  end
end
