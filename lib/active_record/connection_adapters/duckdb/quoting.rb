# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Quoting
        extend ActiveSupport::Concern

        module ClassMethods
          # Quotes a column name for use in SQL statements
          # @param name [String, Symbol] The column name to quote
          # @return [String] The quoted column name wrapped in double quotes
          def quote_column_name(name)
            %("#{name}")
          end
        end

        # Quotes a table name for use in SQL statements
        # @param name [String, Symbol] The table name to quote
        # @return [String] The quoted table name (delegates to quote_column_name)

        def quote_table_name(name)
          quote_column_name(name)
        end

        # Quotes a column name for use in SQL statements
        # @param name [String, Symbol] The column name to quote
        # @return [String] The quoted column name wrapped in double quotes
        def quote_column_name(name)
          %("#{name}")
        end

        # Quotes a value for safe inclusion in SQL statements
        # @param value [Object] The value to quote
        # @return [String] The appropriately quoted value for SQL
        def quote(value)
          case value
          when String
            "'#{value.gsub("'", "''")}'"
          when nil
            'NULL'
          when true
            'TRUE'
          when false
            'FALSE'
          when Numeric
            value.to_s
          when Time, DateTime
            "'#{value.utc.strftime("%Y-%m-%d %H:%M:%S")}'"
          when Date
            "'#{value.strftime("%Y-%m-%d")}'"
          else
            "'#{value.to_s.gsub("'", "''")}'"
          end
        end
      end
    end
  end
end
