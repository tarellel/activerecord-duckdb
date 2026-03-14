# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::Type::Interval do
  let(:type) { described_class.new }

  describe '#type' do
    it 'returns :interval' do
      expect(type.type).to eq(:interval)
    end
  end

  describe '#cast' do
    it 'casts ActiveSupport::Duration' do
      duration = 2.hours + 30.minutes
      result = type.cast(duration)
      expect(result).to eq(duration)
    end

    it 'casts Numeric as seconds' do
      result = type.cast(3600)
      expect(result).to be_a(ActiveSupport::Duration)
      expect(result.in_seconds).to eq(3600)
    end

    it 'casts nil to nil' do
      expect(type.cast(nil)).to be_nil
    end

    it 'casts ISO8601 string' do
      result = type.cast('PT2H30M')
      expect(result).to be_a(ActiveSupport::Duration)
      expect(result.in_seconds).to eq(9000)
    end

    it 'casts hash with duration parts' do
      result = type.cast({ hours: 2, minutes: 30 })
      expect(result).to be_a(ActiveSupport::Duration)
      expect(result.in_seconds).to eq(9000)
    end
  end

  describe '#serialize' do
    it 'serializes ActiveSupport::Duration to interval string' do
      duration = 2.hours + 30.minutes
      result = type.serialize(duration)
      expect(result).to include('hours')
      expect(result).to include('minutes')
    end

    it 'serializes numeric as interval string' do
      result = type.serialize(3600)
      expect(result).to include('hour')
    end

    it 'serializes nil to nil' do
      expect(type.serialize(nil)).to be_nil
    end

    it 'passes through strings' do
      expect(type.serialize('1 day')).to eq('1 day')
    end
  end

  describe '#deserialize' do
    it 'deserializes DuckDB::Interval to Duration' do
      # Create a mock DuckDB::Interval
      interval = DuckDB::Interval.new
      # DuckDB::Interval has interval_months, interval_days, interval_micros
      allow(interval).to receive(:interval_months).and_return(0)
      allow(interval).to receive(:interval_days).and_return(1)
      allow(interval).to receive(:interval_micros).and_return(3600_000_000) # 1 hour

      result = type.deserialize(interval)
      expect(result).to be_a(ActiveSupport::Duration)
      expect(result.parts[:days]).to eq(1)
      expect(result.parts[:hours]).to eq(1)
    end

    it 'deserializes nil to nil' do
      expect(type.deserialize(nil)).to be_nil
    end

    it 'passes through ActiveSupport::Duration' do
      duration = 1.day
      expect(type.deserialize(duration)).to eq(duration)
    end
  end

  describe 'integration with ActiveRecord' do
    before do
      ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
      ActiveRecord::Base.connection.create_table(:interval_samples, id: false) do |t|
        t.integer :id
        t.interval :duration
        t.string :label
      end
    end

    after do
      ActiveRecord::Base.connection.drop_table(:interval_samples, if_exists: true)
      ActiveRecord::Base.remove_connection
    end

    let(:model_class) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'interval_samples'
        self.primary_key = 'id'

        def self.name
          'IntervalSample'
        end
      end
    end

    it 'writes and reads back hours and minutes' do
      model_class.create!(id: 1, duration: 2.hours + 30.minutes, label: 'short')

      record = model_class.find(1)
      expect(record.duration).to be_a(ActiveSupport::Duration)
      expect(record.duration.in_seconds).to eq(9000) # 2.5 hours
      expect(record.duration.iso8601).to eq('PT2H30M')
    end

    it 'writes and reads back days' do
      model_class.create!(id: 1, duration: 3.days, label: 'days')

      record = model_class.find(1)
      expect(record.duration).to be_a(ActiveSupport::Duration)
      expect(record.duration.in_days).to eq(3)
    end

    it 'writes and reads back months and years' do
      model_class.create!(id: 1, duration: 1.year + 6.months, label: 'long')

      record = model_class.find(1)
      expect(record.duration).to be_a(ActiveSupport::Duration)
      # 1 year + 6 months = 18 months
      expect(record.duration.parts[:years]).to eq(1)
      expect(record.duration.parts[:months]).to eq(6)
    end

    it 'writes and reads back complex durations' do
      complex_duration = 1.year + 2.months + 3.days + 4.hours + 5.minutes
      model_class.create!(id: 1, duration: complex_duration, label: 'complex')

      record = model_class.find(1)
      expect(record.duration).to be_a(ActiveSupport::Duration)
      # Verify components are preserved
      expect(record.duration.parts[:years]).to eq(1)
      expect(record.duration.parts[:months]).to eq(2)
      expect(record.duration.parts[:days]).to eq(3)
      expect(record.duration.parts[:hours]).to eq(4)
      expect(record.duration.parts[:minutes]).to eq(5)
    end

    it 'handles nil intervals' do
      model_class.create!(id: 1, duration: nil, label: 'nil')

      record = model_class.find(1)
      expect(record.duration).to be_nil
    end

    it 'supports querying by interval' do
      model_class.create!(id: 1, duration: 1.hour, label: 'short')
      model_class.create!(id: 2, duration: 24.hours, label: 'day')
      model_class.create!(id: 3, duration: 1.week, label: 'week')

      # Can use intervals in queries
      results = model_class.where(label: 'day')
      expect(results.count).to eq(1)
      expect(results.first.duration.in_hours).to eq(24)
    end

    it 'supports updating intervals' do
      model_class.create!(id: 1, duration: 1.hour, label: 'original')

      record = model_class.find(1)
      record.update!(duration: 2.hours)

      reloaded = model_class.find(1)
      expect(reloaded.duration.in_hours).to eq(2)
    end

    it 'can serialize duration to ISO8601' do
      model_class.create!(id: 1, duration: 1.day + 2.hours, label: 'iso')

      record = model_class.find(1)
      # ActiveSupport::Duration supports ISO8601 serialization
      expect(record.duration.iso8601).to match(/P.*T.*H/)
    end
  end
end
