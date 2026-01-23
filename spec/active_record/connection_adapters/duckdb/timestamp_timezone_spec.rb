# frozen_string_literal: true

require 'spec_helper'

# Shared examples for timestamp handling with ActiveRecord.default_timezone = :utc
# Requires `setup_table` lambda to be defined via `let`
# No explicit teardown needed - in-memory DuckDB tables are discarded with connection,
# and DuckLake files are cleaned up via after hooks
RSpec.shared_examples 'correct UTC timestamp handling' do
  it 'normalizes TIMESTAMP to UTC' do
    connection = ActiveRecord::Base.connection
    table_name = "ts_norm_test_#{SecureRandom.hex(4)}"

    setup_table.call(connection, table_name, 'id INTEGER, recorded_at TIMESTAMP')

    utc_time = Time.utc(2025, 1, 15, 12, 0, 0)
    connection.execute("INSERT INTO #{table_name} VALUES (1, '2025-01-15 12:00:00')")

    result = connection.send(:internal_exec_query, "SELECT recorded_at FROM #{table_name} WHERE id = 1")
    retrieved_time = result.rows.first.first

    expect(retrieved_time).to be_a(Time)
    expect(retrieved_time.utc?).to eq(true), "TIMESTAMP should be normalized to UTC"
    expect(retrieved_time.to_i).to eq(utc_time.to_i),
      "Timestamp mismatch: expected #{utc_time.to_i}, got #{retrieved_time.to_i}"
  end

  it 'preserves TIMESTAMPTZ with zero UTC offset' do
    connection = ActiveRecord::Base.connection
    table_name = "tstz_test_#{SecureRandom.hex(4)}"

    setup_table.call(connection, table_name, 'id INTEGER, event_at TIMESTAMPTZ')

    connection.execute("INSERT INTO #{table_name} VALUES (1, '2025-01-15 12:00:00+00')")

    result = connection.send(:internal_exec_query, "SELECT event_at FROM #{table_name} WHERE id = 1")
    retrieved_time = result.rows.first.first

    expect(retrieved_time).to be_a(Time)
    expect(retrieved_time.utc_offset).to eq(0), "TIMESTAMPTZ should have zero UTC offset"
    expect(retrieved_time.hour).to eq(12)
  end

  it 'correctly round-trips TIMESTAMP and TIMESTAMPTZ' do
    connection = ActiveRecord::Base.connection
    table_name = "roundtrip_test_#{SecureRandom.hex(4)}"

    setup_table.call(connection, table_name, 'id INTEGER, ts TIMESTAMP, tstz TIMESTAMPTZ')

    test_times = [
      Time.utc(2025, 1, 15, 0, 0, 0),    # Midnight UTC
      Time.utc(2025, 6, 15, 12, 0, 0),   # Summer (DST period)
      Time.utc(2025, 12, 31, 23, 59, 59) # End of year
    ]

    test_times.each_with_index do |original_time, idx|
      ts_str = original_time.strftime('%Y-%m-%d %H:%M:%S')
      connection.execute("INSERT INTO #{table_name} VALUES (#{idx}, '#{ts_str}', '#{ts_str}+00')")
    end

    test_times.each_with_index do |original_time, idx|
      result = connection.send(:internal_exec_query, "SELECT ts, tstz FROM #{table_name} WHERE id = #{idx}")
      row = result.rows.first
      ts_value = row[0]
      tstz_value = row[1]

      expect(ts_value.to_i).to eq(original_time.to_i),
        "TIMESTAMP round-trip failed for #{original_time}"
      expect(tstz_value.to_i).to eq(original_time.to_i),
        "TIMESTAMPTZ round-trip failed for #{original_time}"
    end
  end
end

# Shared examples for timestamp handling with ActiveRecord.default_timezone = :local
# Requires `setup_table` lambda to be defined via `let`
RSpec.shared_examples 'correct local timestamp handling' do
  it 'returns TIMESTAMP in local time' do
    connection = ActiveRecord::Base.connection
    table_name = "ts_local_test_#{SecureRandom.hex(4)}"

    setup_table.call(connection, table_name, 'id INTEGER, recorded_at TIMESTAMP')

    # Insert a timestamp without timezone info
    connection.execute("INSERT INTO #{table_name} VALUES (1, '2025-01-15 12:00:00')")

    result = connection.send(:internal_exec_query, "SELECT recorded_at FROM #{table_name} WHERE id = 1")
    retrieved_time = result.rows.first.first

    expect(retrieved_time).to be_a(Time)
    # When default_timezone is :local, timestamps should be in local time
    expect(retrieved_time.utc?).to eq(false), "TIMESTAMP should be in local time, not UTC"
    expect(retrieved_time.hour).to eq(12)
    expect(retrieved_time.min).to eq(0)
  end

  it 'correctly round-trips TIMESTAMP in local timezone mode' do
    connection = ActiveRecord::Base.connection
    table_name = "ts_roundtrip_local_#{SecureRandom.hex(4)}"

    setup_table.call(connection, table_name, 'id INTEGER, ts TIMESTAMP')

    # Use local time for the test
    local_time = Time.local(2025, 1, 15, 12, 0, 0)
    ts_str = local_time.strftime('%Y-%m-%d %H:%M:%S')
    connection.execute("INSERT INTO #{table_name} VALUES (1, '#{ts_str}')")

    result = connection.send(:internal_exec_query, "SELECT ts FROM #{table_name} WHERE id = 1")
    retrieved_time = result.rows.first.first

    # The retrieved time components should match what we inserted
    expect(retrieved_time.year).to eq(2025)
    expect(retrieved_time.month).to eq(1)
    expect(retrieved_time.day).to eq(15)
    expect(retrieved_time.hour).to eq(12)
    expect(retrieved_time.min).to eq(0)
    expect(retrieved_time.sec).to eq(0)
  end
end

RSpec.describe 'Timestamp timezone handling' do
  include TestHelpers

  before do
    establish_test_connection
  end

  after do
    remove_test_connection
  end

  describe 'timestamp storage and retrieval' do
    context 'with Rails timezone set to UTC' do
      around do |example|
        # Save the original timezone settings
        original_zone = Time.zone
        original_default_timezone = ActiveRecord.default_timezone

        # Set Rails to use UTC (standard Rails behavior)
        Time.zone = 'UTC'
        ActiveRecord.default_timezone = :utc

        example.run
      ensure
        # Restore original settings
        Time.zone = original_zone
        ActiveRecord.default_timezone = original_default_timezone
      end

      it 'stores and retrieves timestamps in UTC correctly' do
        with_test_model(:timestamp_tests, table_definition: ->(t) { t.datetime :recorded_at }) do |model|
          # Create a specific UTC time
          utc_time = Time.utc(2025, 1, 15, 12, 0, 0)

          # Store the time
          record = model.create!(recorded_at: utc_time)
          record.reload

          # The retrieved time should match the original UTC time
          retrieved_time = record.recorded_at

          expect(retrieved_time.year).to eq(2025)
          expect(retrieved_time.month).to eq(1)
          expect(retrieved_time.day).to eq(15)
          expect(retrieved_time.hour).to eq(12)
          expect(retrieved_time.min).to eq(0)
          expect(retrieved_time.sec).to eq(0)

          # Critical: The timestamp should be in UTC, not local time
          # This is the bug - duckdb gem parses as local time, introducing an offset
          expect(retrieved_time.utc?).to eq(true), "Expected timestamp to be in UTC, but got #{retrieved_time.zone}"
          expect(retrieved_time).to eq(utc_time)
        end
      end

      it 'handles Time.current correctly' do
        with_test_model(:timestamp_tests, table_definition: ->(t) { t.datetime :recorded_at }) do |model|
          # Time.current returns an ActiveSupport::TimeWithZone in the configured timezone
          current_time = Time.current.change(usec: 0) # Normalize to second precision

          record = model.create!(recorded_at: current_time)
          record.reload

          retrieved_time = record.recorded_at

          # The retrieved time should match the original time (in UTC)
          expect(retrieved_time).to eq(current_time.utc)
        end
      end

      it 'correctly round-trips timestamps without timezone offset' do
        with_test_model(:timestamp_tests, table_definition: ->(t) { t.datetime :recorded_at }) do |model|
          # Use a time that clearly shows offset issues
          # If we're in a timezone that's UTC-5, a bug would show up as:
          # - Write: 2025-01-15 12:00:00 UTC
          # - Read: 2025-01-15 12:00:00 (local) = 2025-01-15 17:00:00 UTC
          test_times = [
            Time.utc(2025, 1, 15, 0, 0, 0),   # Midnight UTC
            Time.utc(2025, 1, 15, 12, 0, 0),  # Noon UTC
            Time.utc(2025, 6, 15, 12, 0, 0),  # Daylight saving time period
            Time.utc(2025, 12, 31, 23, 59, 59) # End of year
          ]

          test_times.each do |original_time|
            record = model.create!(recorded_at: original_time)
            record.reload
            retrieved_time = record.recorded_at

            expect(retrieved_time.to_i).to eq(original_time.to_i),
              "Timestamp mismatch for #{original_time}: expected epoch #{original_time.to_i}, got #{retrieved_time.to_i} " \
              "(difference: #{retrieved_time.to_i - original_time.to_i} seconds)"
          end
        end
      end
    end

    context 'with local timezone configured' do
      include_context 'with Europe/Berlin timezone'

      it 'stores local time as UTC and retrieves correctly' do
        with_test_model(:timestamp_tests, table_definition: ->(t) { t.datetime :recorded_at }) do |model|
          # Create a time in local timezone
          local_time = Time.zone.local(2025, 1, 15, 12, 0, 0)
          utc_equivalent = local_time.utc

          record = model.create!(recorded_at: local_time)
          record.reload

          retrieved_time = record.recorded_at

          # Should match the UTC equivalent
          expect(retrieved_time.to_i).to eq(utc_equivalent.to_i),
            "Expected #{utc_equivalent} (epoch: #{utc_equivalent.to_i}), " \
            "got #{retrieved_time} (epoch: #{retrieved_time.to_i})"
        end
      end
    end

    context 'raw SQL timestamp handling' do
      it 'stores and retrieves timestamps via raw SQL correctly' do
        connection = ActiveRecord::Base.connection

        connection.execute('CREATE TABLE raw_timestamp_test (id INTEGER, ts TIMESTAMP)')

        # Insert a specific timestamp
        connection.execute("INSERT INTO raw_timestamp_test VALUES (1, '2025-01-15 12:00:00')")

        result = connection.execute('SELECT ts FROM raw_timestamp_test WHERE id = 1')
        raw_timestamp = result.first.first

        # If the duckdb gem returns a Time object, check if it's UTC
        if raw_timestamp.is_a?(Time)
          # This is the core issue: is the time interpreted as UTC or local?
          # The string '2025-01-15 12:00:00' should be UTC since that's what Rails stores
          expect(raw_timestamp.hour).to eq(12), 
            "Expected hour to be 12, got #{raw_timestamp.hour}. " \
            "This indicates the timestamp is being interpreted in the wrong timezone."
        end

        connection.execute('DROP TABLE raw_timestamp_test')
      end
    end

    context 'verifying the timezone offset bug' do
      it 'demonstrates the timezone offset issue' do
        # This test demonstrates the bug when running in a non-UTC timezone
        # Skip if already running in UTC
        local_offset = Time.now.utc_offset

        if local_offset == 0
          skip 'Running in UTC timezone - offset bug not demonstrable'
        end

        with_test_model(:timestamp_tests, table_definition: ->(t) { t.datetime :recorded_at }) do |model|
          # Store a UTC time
          utc_time = Time.utc(2025, 1, 15, 12, 0, 0)
          
          record = model.create!(recorded_at: utc_time)
          record.reload

          retrieved_time = record.recorded_at

          # Calculate what the bug would produce:
          # If duckdb parses "2025-01-15 12:00:00" as local time instead of UTC,
          # converting that local time back to UTC would add the local offset
          buggy_time_epoch = utc_time.to_i + local_offset
          
          # Check if we have the bug
          has_bug = (retrieved_time.to_i == buggy_time_epoch)
          
          if has_bug
            puts "\n⚠️  TIMEZONE BUG DETECTED!"
            puts "   Original UTC time: #{utc_time} (epoch: #{utc_time.to_i})"
            puts "   Retrieved time:    #{retrieved_time} (epoch: #{retrieved_time.to_i})"
            puts "   Local offset:      #{local_offset} seconds (#{local_offset / 3600.0} hours)"
            puts "   Difference:        #{retrieved_time.to_i - utc_time.to_i} seconds"
          end

          # This assertion will fail if the bug exists
          expect(retrieved_time.to_i).to eq(utc_time.to_i),
            "Timestamp offset bug detected! " \
            "Expected #{utc_time.to_i}, got #{retrieved_time.to_i}. " \
            "Difference: #{retrieved_time.to_i - utc_time.to_i} seconds " \
            "(local offset is #{local_offset} seconds)"
        end
      end
    end

    context 'timestamps column (created_at, updated_at)' do
      it 'correctly handles Rails timestamps columns' do
        with_test_model(:timestamp_tests, table_definition: ->(t) { 
          t.string :name
          t.timestamps 
        }) do |model|
          # Freeze time for predictable testing
          frozen_time = Time.utc(2025, 1, 15, 12, 0, 0)

          record = nil
          # Use travel_to if available, otherwise just test what we can
          if defined?(ActiveSupport::Testing::TimeHelpers)
            extend ActiveSupport::Testing::TimeHelpers
            travel_to(frozen_time) do
              record = model.create!(name: 'test')
            end
          else
            record = model.create!(name: 'test')
          end

          record.reload

          # created_at and updated_at should be in UTC
          expect(record.created_at).to be_a(Time)
          expect(record.updated_at).to be_a(Time)

          # If we froze time, verify the exact values
          if defined?(ActiveSupport::Testing::TimeHelpers)
            expect(record.created_at.to_i).to eq(frozen_time.to_i)
            expect(record.updated_at.to_i).to eq(frozen_time.to_i)
          end
        end
      end
    end

    context 'TIMESTAMP and TIMESTAMPTZ with plain DuckDB' do
      include_context 'with Europe/Berlin timezone'

      # Setup for plain DuckDB tables (no teardown needed - in-memory DB is discarded)
      let(:setup_table) do
        ->(connection, table_name, columns) {
          connection.execute("CREATE TABLE #{table_name} (#{columns})")
        }
      end

      include_examples 'correct UTC timestamp handling'

      it 'preserves TIMESTAMPTZ timezone semantics for different offsets' do
        connection = ActiveRecord::Base.connection

        connection.execute('CREATE TABLE timestamptz_offset_test (id INTEGER, tstz TIMESTAMPTZ)')

        # Insert timestamps with different timezone offsets
        test_cases = [
          { offset: '+00', expected_utc_hour: 12 },
          { offset: '+01', expected_utc_hour: 11 }, # 12:00+01 = 11:00 UTC
          { offset: '-05', expected_utc_hour: 17 }, # 12:00-05 = 17:00 UTC
        ]

        test_cases.each_with_index do |test_case, idx|
          connection.execute(
            "INSERT INTO timestamptz_offset_test VALUES (#{idx}, '2025-01-15 12:00:00#{test_case[:offset]}')"
          )
        end

        test_cases.each_with_index do |test_case, idx|
          result = connection.send(
            :internal_exec_query,
            "SELECT tstz FROM timestamptz_offset_test WHERE id = #{idx}"
          )
          tstz_value = result.rows.first.first

          expect(tstz_value).to be_a(Time)
          expect(tstz_value.hour).to eq(test_case[:expected_utc_hour]),
            "TIMESTAMPTZ with offset #{test_case[:offset]} should have UTC hour #{test_case[:expected_utc_hour]}, " \
            "got #{tstz_value.hour}"
        end
      end
    end
  end

  describe 'DuckLake timestamp handling' do
    # DuckLake is a lakehouse extension for DuckDB
    # These tests verify timestamp handling works correctly with DuckLake databases

    include_context 'with Europe/Berlin timezone'

    # Track active DuckLake database for cleanup
    let(:ducklake_db_name) { "ducklake_#{SecureRandom.hex(4)}" }

    # Setup for DuckLake tables
    let(:setup_table) do
      ->(connection, table_name, columns) {
        skip 'DuckLake extension not available' unless connection.ducklake_extension_available?

        data_path = File.join('tmp', "#{ducklake_db_name}_data_#{Process.pid}")
        metadata_path = File.join('tmp', "#{ducklake_db_name}_metadata_#{Process.pid}.duckdb")

        @ducklake_data_path = data_path
        @ducklake_metadata_path = metadata_path

        connection.send(:with_raw_connection) do |raw_conn|
          raw_conn.execute(
            "ATTACH '#{metadata_path}' AS #{ducklake_db_name} (TYPE DUCKLAKE, DATA_PATH '#{data_path}')"
          )
          raw_conn.execute("USE #{ducklake_db_name}")
        end

        connection.execute("CREATE TABLE #{table_name} (#{columns})")
      }
    end

    # No per-test cleanup needed - connection close releases attached databases
    # Suite-level cleanup in spec_helper removes tmp files after all tests complete

    include_examples 'correct UTC timestamp handling'
  end

  describe 'with ActiveRecord.default_timezone = :local' do
    around do |example|
      original_zone = Time.zone
      original_default_timezone = ActiveRecord.default_timezone

      # Configure for local timezone mode
      Time.zone = 'Europe/Berlin'
      ActiveRecord.default_timezone = :local

      example.run
    ensure
      Time.zone = original_zone
      ActiveRecord.default_timezone = original_default_timezone
    end

    # Setup for plain DuckDB tables
    let(:setup_table) do
      ->(connection, table_name, columns) {
        connection.execute("CREATE TABLE #{table_name} (#{columns})")
      }
    end

    include_examples 'correct local timestamp handling'
  end
end
