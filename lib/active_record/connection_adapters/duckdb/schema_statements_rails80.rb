# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # Rails 7.2 and 8.0 specific schema statement implementations.
      # Column constructor: (name, default, sql_type_metadata, null, default_function, **options)
      module SchemaStatementsRails80
        # Creates a new Column object from DuckDB field information.
        # @param table_name [String] The name of the table
        # @param field [Array] Array containing column field information from PRAGMA table_info
        # @param definitions [Hash] Additional column definitions (unused)
        # @return [ActiveRecord::ConnectionAdapters::Duckdb::Column] The created column object
        def new_column_from_field(table_name, field, _definitions)
          info = column_info_from_field(table_name, field)

          Column.new(
            info[:name],
            info[:default],
            info[:sql_type_metadata],
            info[:null],
            info[:default_function],
            collation: info[:collation],
            comment: info[:comment],
            auto_increment: info[:auto_increment],
            rowid: info[:rowid]
          )
        end
      end
    end
  end
end
