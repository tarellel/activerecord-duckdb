# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      class Column < ConnectionAdapters::Column
        # Initialize a new DuckDB column with DuckDB-specific attributes
        # @param auto_increment [Boolean, nil] whether this column is auto-incrementing
        # @param rowid [Boolean, nil] whether this column is a rowid column
        # @param generated_type [Symbol, nil] the type of generated column (:stored, :virtual)
        # @param extra [String, nil] extra column definition information
        def initialize(*, auto_increment: nil, rowid: nil, generated_type: nil, extra: nil, **)
          super(*, **)
          @auto_increment = auto_increment
          @rowid = rowid
          @generated_type = generated_type
          @extra = extra
        end

        # Quotes a column name for use in SQL statements
        # @param name [String, Symbol] The column name to quote
        # @return [String] The quoted column name wrapped in double quotes
        def quote_column_name(name)
          %("#{name}")
        end

        # @return [String, nil] extra column definition information
        attr_reader :extra
        # @return [Boolean, nil] whether this column is a rowid column
        attr_reader :rowid

        # Check if this column is a virtual/generated column
        # https://duckdb.org/docs/stable/sql/statements/create_table#generated-columns
        # TODO: Implement full virtual column support
        # @return [Boolean] always returns false until virtual columns are fully implemented
        def virtual?
          # /\b(?:VIRTUAL|STORED|GENERATED)\b/.match?(extra)
          false
        end

        # def virtual_stored?
        #   virtual? && @generated_type == :stored
        # end

        # Check if this column has a default value
        # @return [Boolean] true if the column has a default value
        def has_default?
          # super && !virtual?
          !!super
        end

        # Check if this column is auto-incrementing
        # We probably need to check if the column is a UUID
        # @return [Boolean] true if the column auto-increments
        def auto_increment?
          !!@auto_increment
        end

        # Check if this column's value is automatically incremented by the database
        # @return [Boolean] true if the column is auto-incremented by the database
        def auto_incremented_by_db?
          auto_increment? || !!rowid
        end
      end
    end
  end
end
