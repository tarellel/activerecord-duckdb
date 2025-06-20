# frozen_string_literal: true

require 'active_record/connection_adapters/abstract/schema_definitions'

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # DuckDB-specific column method definitions for table creation
      # Provides both standard Rails column types and DuckDB-specific data types
      module ColumnMethods
        extend ActiveSupport::Concern
        extend ConnectionAdapters::ColumnMethods::ClassMethods

        define_column_methods(
          # binary
          :blob,
          # Integer variants
          :tinyint,           # TINYINT (1 byte: -128 to 127)
          :smallint,          # SMALLINT (2 bytes: -32,768 to 32,767)
          :hugeint,           # HUGEINT (16 bytes: very large integers)
          :varint,            # VARINT (variable precision integer, up to 1.2M digits)
          # Unsigned integer variants
          :utinyint,          # UTINYINT (1 byte: 0 to 255)
          :usmallint,         # USMALLINT (2 bytes: 0 to 65,535)
          :uinteger,          # UINTEGER (4 bytes: 0 to 4,294,967,295)
          :ubigint,           # UBIGINT (8 bytes: 0 to 18,446,744,073,709,551,615)
          :uhugeint,          # UHUGEINT (16 bytes: 0 to 2^128-1)
          # Special data types
          :uuid,              # UUID type for unique identifiers
          :interval,          # INTERVAL for time periods
          :bit,               # BIT for bit strings
          # Complex/nested types
          :list,              # LIST (variable-length array)
          :struct,            # STRUCT (composite type with named fields)
          :map,               # MAP (key-value pairs)
          :union,             # UNION (value can be one of several types)
          :enum # ENUM (predefined set of values)
          # JSON can't be a column type, but can be a queried type of data
          # :json # JSON documents (check DuckDB version compatibility)
        )

        alias binary blob

        # Creates a LIST column with specified element type
        # @param name [String, Symbol] The column name
        # @param element_type [Symbol] The type of elements in the list (default: :string)
        # @return [void]
        # @example Create a list of strings
        #   t.list :tags, element_type: :string
        def list(name, element_type: :string, **)
          column(name, "#{element_type.to_s.upcase}[]", **)
        end

        # Creates a STRUCT column with named fields
        # @param name [String, Symbol] The column name
        # @param fields [Hash] Hash mapping field names to their types (default: {})
        # @return [void]
        # @example Create an address struct
        #   t.struct :address, fields: { street: :string, city: :string, zip: :integer }
        def struct(name, fields: {}, **)
          field_definitions = fields.map { |field_name, field_type| "#{field_name} #{field_type.to_s.upcase}" }
          column(name, "STRUCT(#{field_definitions.join(", ")})", **)
        end

        # Creates a MAP column with specified key and value types
        # @param name [String, Symbol] The column name
        # @param key_type [Symbol] The type of map keys (default: :string)
        # @param value_type [Symbol] The type of map values (default: :string)
        # @return [void]
        # @example Create a string-to-string map
        #   t.map :metadata, key_type: :string, value_type: :string
        def map(name, key_type: :string, value_type: :string, **)
          column(name, "MAP(#{key_type.to_s.upcase}, #{value_type.to_s.upcase})", **)
        end

        # Creates an ENUM column with predefined values
        # @param name [String, Symbol] The column name
        # @param values [Array] Array of allowed enum values (default: [])
        # @return [void]
        # @example Create a status enum
        #   t.enum :status, values: ['active', 'inactive', 'pending']
        def enum(name, values: [], **)
          enum_values = values.map { |v| "'#{v}'" }.join(', ')
          column(name, "ENUM(#{enum_values})", **)
        end
      end

      # DuckDB-specific table definition for CREATE TABLE statements
      # Extends Rails' TableDefinition with DuckDB column types and features
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
        include Duckdb::ColumnMethods

        # Initialize a new DuckDB table definition
        # @param conn [ActiveRecord::ConnectionAdapters::DuckdbAdapter] The database adapter
        # @param name [String, Symbol] The table name
        # @param temporary [Boolean] Whether this is a temporary table
        # @param if_not_exists [Boolean] Whether to use IF NOT EXISTS clause
        # @param options [Hash, nil] Additional table options
        # @param as [String, nil] SELECT statement for CREATE TABLE AS
        # @param comment [String, nil] Table comment
        # @param table_options [Hash] Additional keyword table options
        def initialize(conn, name, temporary: false, if_not_exists: false,
                       options: nil, as: nil, comment: nil, **table_options)
          super
          @conn = conn
          @table_name = name
        end

        # Creates a column definition for the table
        # Note: sequence defaults are handled by ALTER TABLE after table creation
        # @param name [String, Symbol] The column name
        # @param type [Symbol] The column type
        # @param index [Boolean, Hash, nil] Whether to create an index on this column
        # @param options [Hash] Additional column options
        # @return [void]
        def column(name, type, index: nil, **options)
          # Don't set sequence defaults here - they're handled in create_table via ALTER TABLE
          super
        end

        # Creates a primary key column definition
        # Note: sequence defaults are handled by ALTER TABLE after table creation
        # @param name [String, Symbol] The primary key column name
        # @param type [Symbol] The primary key column type (default: :primary_key)
        # @param options [Hash] Additional column options
        # @return [void]
        def primary_key(name, type = :primary_key, **options)
          # Don't set sequence defaults here - they're handled in create_table via ALTER TABLE
          super
        end
      end

      # DuckDB-specific table modification for ALTER TABLE statements
      # Extends Rails' Table with DuckDB column types and features
      class Table < ActiveRecord::ConnectionAdapters::Table
        include Duckdb::ColumnMethods
      end

      # DuckDB-specific table alteration functionality
      # Extends Rails' AlterTable for DuckDB-specific schema changes
      class AlterTable < ActiveRecord::ConnectionAdapters::AlterTable
      end
    end
  end
end
