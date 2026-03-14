# frozen_string_literal: true

require 'active_support/duration'

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Type
        # Type for DuckDB INTERVAL columns
        # Converts between DuckDB::Interval and ActiveSupport::Duration
        #
        # @example Reading an interval from the database
        #   # DuckDB returns: DuckDB::Interval with months=0, days=1, micros=3600000000
        #   # Ruby receives: ActiveSupport::Duration of "1 day and 1 hour"
        #
        # @example Writing an interval to the database
        #   # Ruby sends: 2.hours + 30.minutes (ActiveSupport::Duration)
        #   # DuckDB receives: "INTERVAL '2 hours 30 minutes'"
        class Interval < ActiveRecord::Type::Value
          def type
            :interval
          end

          # Deserialize from database value to Ruby object
          # @param value [DuckDB::Interval, String, nil] The database value
          # @return [ActiveSupport::Duration, nil] The Ruby duration
          def deserialize(value)
            return nil if value.nil?

            case value
            when ::DuckDB::Interval
              interval_to_duration(value)
            when ::ActiveSupport::Duration
              value
            when ::String
              parse_interval_string(value)
            else
              nil
            end
          end

          # Cast from user input to Ruby object
          # @param value [Object] The user input
          # @return [ActiveSupport::Duration, nil] The Ruby duration
          def cast(value)
            return nil if value.nil?

            case value
            when ::ActiveSupport::Duration
              value
            when ::DuckDB::Interval
              interval_to_duration(value)
            when ::Numeric
              # Treat numeric as seconds
              ActiveSupport::Duration.build(value)
            when ::String
              parse_interval_string(value)
            when ::Hash
              # Allow hash like { hours: 2, minutes: 30 }
              hash_to_duration(value)
            else
              nil
            end
          end

          # Serialize from Ruby object to database value
          # @param value [ActiveSupport::Duration, Numeric, nil] The Ruby value
          # @return [String, nil] The SQL interval literal
          def serialize(value)
            return nil if value.nil?

            case value
            when ::ActiveSupport::Duration
              duration_to_interval_string(value)
            when ::Numeric
              duration_to_interval_string(ActiveSupport::Duration.build(value))
            when ::String
              value
            else
              nil
            end
          end

          private

          # Convert DuckDB::Interval to ActiveSupport::Duration
          def interval_to_duration(interval)
            parts = []

            if interval.interval_months != 0
              years, months = interval.interval_months.divmod(12)
              parts << years.years if years != 0
              parts << months.months if months != 0
            end

            parts << interval.interval_days.days if interval.interval_days != 0

            if interval.interval_micros != 0
              total_seconds = interval.interval_micros / 1_000_000.0
              hours, remainder = total_seconds.divmod(3600)
              minutes, seconds = remainder.divmod(60)

              parts << hours.to_i.hours if hours >= 1
              parts << minutes.to_i.minutes if minutes >= 1
              parts << seconds.seconds if seconds > 0
            end

            return 0.seconds if parts.empty?

            parts.reduce(:+)
          end

          # Convert ActiveSupport::Duration to DuckDB interval string
          def duration_to_interval_string(duration)
            parts = []
            remaining = duration.in_seconds

            # Extract components
            if duration.parts[:years]
              parts << "#{duration.parts[:years]} years"
            end

            if duration.parts[:months]
              parts << "#{duration.parts[:months]} months"
            end

            if duration.parts[:weeks]
              parts << "#{duration.parts[:weeks]} weeks"
            end

            if duration.parts[:days]
              parts << "#{duration.parts[:days]} days"
            end

            if duration.parts[:hours]
              parts << "#{duration.parts[:hours]} hours"
            end

            if duration.parts[:minutes]
              parts << "#{duration.parts[:minutes]} minutes"
            end

            if duration.parts[:seconds]
              parts << "#{duration.parts[:seconds]} seconds"
            end

            # If no parts but has value, convert from total seconds
            if parts.empty? && remaining > 0
              hours, remainder = remaining.divmod(3600)
              minutes, seconds = remainder.divmod(60)

              parts << "#{hours.to_i} hours" if hours >= 1
              parts << "#{minutes.to_i} minutes" if minutes >= 1
              parts << "#{seconds} seconds" if seconds > 0
            end

            parts.empty? ? '0 seconds' : parts.join(' ')
          end

          # Parse interval string to Duration
          def parse_interval_string(str)
            return nil if str.nil? || str.empty?

            # Try ISO8601 format first
            begin
              return ActiveSupport::Duration.parse(str)
            rescue ActiveSupport::Duration::ISO8601Parser::ParsingError
              # Continue to try other formats
            end

            # Parse DuckDB-style interval strings like "1 day 2 hours"
            parts = []
            str.scan(/(\d+)\s*(year|month|week|day|hour|minute|second)s?/i) do |amount, unit|
              parts << amount.to_i.public_send(unit.downcase.pluralize)
            end

            parts.empty? ? nil : parts.reduce(:+)
          end

          # Convert hash to Duration
          def hash_to_duration(hash)
            parts = []
            hash.each do |unit, amount|
              next if amount.nil? || amount == 0

              parts << amount.public_send(unit.to_s.singularize.pluralize)
            end
            parts.empty? ? 0.seconds : parts.reduce(:+)
          end
        end
      end
    end
  end
end
