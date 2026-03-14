# frozen_string_literal: true

# Monkey patch DuckDB::Converter to respect ActiveRecord.default_timezone.
# Based on: https://github.com/suketa/ruby-duckdb/blob/master/lib/duckdb/converter.rb
# Change: Use Time.utc instead of Time.local when ActiveRecord.default_timezone is :utc
# This fixes an issue where timestamps are interpreted in local timezone instead of UTC.
#
# Run `bin/verify_timestamp_patch` to test compatibility with new versions.
module DuckDBConverterTimestampMonkeyPatch
  # Version range verified by bin/verify_timestamp_patch
  TESTED_VERSION_MIN = '1.0.0.0'
  TESTED_VERSION_MAX = '1.4.3.0'

  EXPECTED_METHODS = {
    _to_time: { arity: 7 },
    _to_time_from_duckdb_time: { arity: 4 },
    _to_time_from_duckdb_timestamp_s: { arity: 1 },
    _to_time_from_duckdb_timestamp_ms: { arity: 1 },
    _to_time_from_duckdb_timestamp_ns: { arity: 1 }
  }.freeze

  class << self
    def apply_patch
      const = find_const
      verify_methods!(const)

      unless duckdb_version_ok?
        version = Gem.loaded_specs['duckdb']&.version
        puts "WARNING: duckdb gem version #{version} is outside tested range " \
             "(#{TESTED_VERSION_MIN} - #{TESTED_VERSION_MAX}). " \
             "Please run bin/verify_timestamp_patch and update the range if compatible."
      end

      const.singleton_class.prepend(ClassMethods)
    end

    private

    def find_const
      Kernel.const_get('DuckDB::Converter')
    rescue NameError
      raise "Could not find DuckDB::Converter when applying timestamp patch. Please investigate."
    end

    def verify_methods!(const)
      EXPECTED_METHODS.each do |method_name, expectations|
        mtd = const.method(method_name)
        unless mtd && mtd.arity == expectations[:arity]
          raise "Could not find method #{method_name} with arity #{expectations[:arity]} " \
                "when patching DuckDB::Converter. Please investigate."
        end
      rescue NameError
        raise "Could not find method #{method_name} when patching DuckDB::Converter. Please investigate."
      end
    end

    def duckdb_version_ok?
      version = Gem.loaded_specs['duckdb']&.version
      return false unless version

      min = Gem::Version.new(TESTED_VERSION_MIN)
      max = Gem::Version.new(TESTED_VERSION_MAX)
      version.between?(min, max)
    end
  end
end

module ClassMethods
  EPOCH_UTC = Time.utc(1970, 1, 1).freeze

  def _to_time(year, month, day, hour, minute, second, microsecond)
    if ActiveRecord.default_timezone == :utc
      Time.utc(year, month, day, hour, minute, second, microsecond)
    else
      super
    end
  end

  def _to_time_from_duckdb_time(hour, minute, second, microsecond)
    if ActiveRecord.default_timezone == :utc
      Time.utc(1970, 1, 1, hour, minute, second, microsecond)
    else
      super
    end
  end

  def _to_time_from_duckdb_timestamp_s(time)
    if ActiveRecord.default_timezone == :utc
      EPOCH_UTC + time
    else
      super
    end
  end

  def _to_time_from_duckdb_timestamp_ms(time)
    if ActiveRecord.default_timezone == :utc
      tm = EPOCH_UTC + (time / 1000)
      Time.utc(tm.year, tm.month, tm.day, tm.hour, tm.min, tm.sec, time % 1000 * 1000)
    else
      super
    end
  end

  def _to_time_from_duckdb_timestamp_ns(time)
    if ActiveRecord.default_timezone == :utc
      tm = EPOCH_UTC + (time / 1_000_000_000)
      Time.utc(tm.year, tm.month, tm.day, tm.hour, tm.min, tm.sec, time % 1_000_000_000 / 1000)
    else
      super
    end
  end
end

DuckDBConverterTimestampMonkeyPatch.apply_patch
