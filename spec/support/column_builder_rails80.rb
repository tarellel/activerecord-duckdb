# frozen_string_literal: true

# Rails 7.2/8.0 column builder
# Column constructor: (name, default, sql_type_metadata, null, default_function, ...)
module ColumnBuilderRails80
  def build_column(name, default, metadata, null = true, **options)
    ActiveRecord::ConnectionAdapters::Duckdb::Column.new(name, default, metadata, null, **options)
  end
end
