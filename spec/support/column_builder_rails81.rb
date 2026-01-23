# frozen_string_literal: true

# Rails 8.1+ column builder
# Column constructor: (name, cast_type, default, sql_type_metadata, null, default_function, ...)
module ColumnBuilderRails81
  # Creates a mock cast_type for Rails 8.1+ tests
  # Rails 8.1's Column calls cast_type.mutable? and cast_type.deserialize(default)
  def mock_cast_type
    double('CastType', mutable?: false).tap do |ct|
      allow(ct).to receive(:deserialize) { |val| val }
    end
  end

  def build_column(name, default, metadata, null = true, **options)
    cast_type = mock_cast_type
    ActiveRecord::ConnectionAdapters::Duckdb::Column.new(name, cast_type, default, metadata, null, **options)
  end
end
