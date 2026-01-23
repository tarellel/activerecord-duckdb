# frozen_string_literal: true

require 'spec_helper'

# Tests for DatabaseStatements module
#
# This spec covers all query execution paths documented in QUERY_EXECUTION_CALL_GRAPH.md:
#
# 1. execute(sql, name) → DuckDB::Result
# 2. internal_exec_query(sql, name, binds, ...) → ActiveRecord::Result
# 3. cast_result(raw_result) → ActiveRecord::Result
# 4. affected_rows(raw_result) → Integer
# 5. exec_delete(sql, name, binds) → Integer
# 6. exec_update (alias of exec_delete)
# 7. exec_query → delegates to internal_exec_query (Rails base class)
# 8. exec_insert → handled by Rails base class via sql_for_insert

RSpec.describe 'DatabaseStatements' do
  before do
    ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
    @connection = ActiveRecord::Base.connection

    # Create test table
    @connection.execute(<<~SQL)
      CREATE TABLE statement_test (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        age INTEGER,
        active BOOLEAN
      )
    SQL

    # Insert test data
    @connection.execute("INSERT INTO statement_test VALUES (1, 'Alice', 25, true)")
    @connection.execute("INSERT INTO statement_test VALUES (2, 'Bob', 30, false)")
    @connection.execute("INSERT INTO statement_test VALUES (3, 'Charlie', 35, true)")
  end

  after do
    @connection.execute('DROP TABLE IF EXISTS statement_test') rescue nil
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
  end

  describe '#execute' do
    it 'returns a DuckDB::Result for SELECT queries' do
      result = @connection.execute('SELECT * FROM statement_test')

      expect(result).to be_a(DuckDB::Result)
      expect(result.to_a.length).to eq(3)
    end

    it 'returns a DuckDB::Result for INSERT queries' do
      result = @connection.execute("INSERT INTO statement_test VALUES (4, 'Diana', 28, true)")

      expect(result).to be_a(DuckDB::Result)
    end

    it 'returns a DuckDB::Result for UPDATE queries' do
      result = @connection.execute("UPDATE statement_test SET age = 26 WHERE name = 'Alice'")

      expect(result).to be_a(DuckDB::Result)
      expect(result.rows_changed).to eq(1)
    end

    it 'returns a DuckDB::Result for DELETE queries' do
      result = @connection.execute("DELETE FROM statement_test WHERE name = 'Alice'")

      expect(result).to be_a(DuckDB::Result)
      expect(result.rows_changed).to eq(1)
    end

    it 'handles queries with no results' do
      result = @connection.execute('SELECT * FROM statement_test WHERE id = 999')

      expect(result).to be_a(DuckDB::Result)
      expect(result.to_a).to be_empty
    end
  end

  describe '#internal_exec_query' do
    it 'returns an ActiveRecord::Result' do
      result = @connection.internal_exec_query('SELECT * FROM statement_test')

      expect(result).to be_a(ActiveRecord::Result)
      expect(result.columns).to include('id', 'name', 'age', 'active')
      expect(result.rows.length).to eq(3)
    end

    it 'executes queries without bind parameters' do
      result = @connection.internal_exec_query('SELECT name FROM statement_test WHERE id = 1')

      expect(result.rows.first).to eq(['Alice'])
    end

    it 'executes queries with bind parameters' do
      bind = ActiveRecord::Relation::QueryAttribute.new('id', 1, ActiveRecord::Type::Integer.new)
      result = @connection.internal_exec_query(
        'SELECT name FROM statement_test WHERE id = ?',
        'SQL',
        [bind]
      )

      expect(result.rows.first).to eq(['Alice'])
    end

    it 'executes queries with multiple bind parameters' do
      binds = [
        ActiveRecord::Relation::QueryAttribute.new('age', 25, ActiveRecord::Type::Integer.new),
        ActiveRecord::Relation::QueryAttribute.new('active', true, ActiveRecord::Type::Boolean.new)
      ]
      result = @connection.internal_exec_query(
        'SELECT name FROM statement_test WHERE age >= ? AND active = ?',
        'SQL',
        binds
      )

      expect(result.rows.length).to eq(2) # Alice (25, true) and Charlie (35, true)
    end

    it 'accepts all Rails version keyword arguments' do
      # Test that the method accepts all kwargs without error
      # Rails 8.0+ uses raw_execute under the hood, Rails 7.2 uses internal_exec_query directly
      kwargs = {
        prepare: false,
        async: false,
        allow_retry: false
      }
      # materialize_transactions was added in Rails 8.1
      kwargs[:materialize_transactions] = true if ActiveRecord::VERSION::MAJOR >= 8

      result = @connection.internal_exec_query('SELECT 1', 'SQL', [], **kwargs)

      expect(result).to be_a(ActiveRecord::Result)
    end
  end

  describe '#cast_result' do
    it 'converts DuckDB::Result to ActiveRecord::Result' do
      raw_result = @connection.execute('SELECT id, name FROM statement_test LIMIT 1')
      result = @connection.cast_result(raw_result)

      expect(result).to be_a(ActiveRecord::Result)
      expect(result.columns).to eq(%w[id name])
      expect(result.rows.first).to eq([1, 'Alice'])
    end

    it 'handles empty results' do
      raw_result = @connection.execute('SELECT * FROM statement_test WHERE id = 999')
      result = @connection.cast_result(raw_result)

      expect(result).to be_a(ActiveRecord::Result)
      expect(result.rows).to be_empty
    end

    it 'handles nil input' do
      result = @connection.cast_result(nil)

      expect(result).to be_a(ActiveRecord::Result)
      expect(result).to be_empty
    end

    it 'preserves column names from result' do
      raw_result = @connection.execute('SELECT id AS user_id, name AS user_name FROM statement_test')
      result = @connection.cast_result(raw_result)

      expect(result.columns).to eq(%w[user_id user_name])
    end
  end

  describe '#affected_rows' do
    it 'returns the number of rows changed for UPDATE' do
      raw_result = @connection.execute("UPDATE statement_test SET age = age + 1 WHERE active = true")

      expect(@connection.affected_rows(raw_result)).to eq(2)
    end

    it 'returns the number of rows changed for DELETE' do
      raw_result = @connection.execute("DELETE FROM statement_test WHERE active = false")

      expect(@connection.affected_rows(raw_result)).to eq(1)
    end

    it 'returns 0 when no rows are affected' do
      raw_result = @connection.execute("UPDATE statement_test SET age = 99 WHERE id = 999")

      expect(@connection.affected_rows(raw_result)).to eq(0)
    end
  end

  describe '#exec_delete' do
    it 'returns the number of deleted rows' do
      count = @connection.exec_delete("DELETE FROM statement_test WHERE name = 'Alice'")

      expect(count).to eq(1)
    end

    it 'handles deletion of multiple rows' do
      count = @connection.exec_delete('DELETE FROM statement_test WHERE active = true')

      expect(count).to eq(2)
    end

    it 'returns 0 when no rows match' do
      count = @connection.exec_delete('DELETE FROM statement_test WHERE id = 999')

      expect(count).to eq(0)
    end

    it 'works with bind parameters' do
      bind = ActiveRecord::Relation::QueryAttribute.new('name', 'Bob', ActiveRecord::Type::String.new)
      count = @connection.exec_delete('DELETE FROM statement_test WHERE name = ?', 'SQL', [bind])

      expect(count).to eq(1)
    end

    it 'works with multiple bind parameters' do
      binds = [
        ActiveRecord::Relation::QueryAttribute.new('age', 30, ActiveRecord::Type::Integer.new),
        ActiveRecord::Relation::QueryAttribute.new('active', false, ActiveRecord::Type::Boolean.new)
      ]
      count = @connection.exec_delete('DELETE FROM statement_test WHERE age >= ? AND active = ?', 'SQL', binds)

      expect(count).to eq(1) # Only Bob (30, false)
    end
  end

  describe '#exec_update' do
    it 'behaves the same as exec_delete' do
      # In Rails 7.2, exec_update is an alias for exec_delete in our adapter
      # In Rails 8.0+, they're separate methods in the base class but behave identically
      if ActiveRecord::VERSION::MAJOR < 8
        expect(@connection.method(:exec_update)).to eq(@connection.method(:exec_delete))
      else
        # Both should return integer row counts
        delete_result = @connection.exec_delete("DELETE FROM statement_test WHERE name = 'NonExistent'")
        update_result = @connection.exec_update("UPDATE statement_test SET age = 99 WHERE name = 'NonExistent'")
        expect(delete_result).to eq(0)
        expect(update_result).to eq(0)
      end
    end

    it 'returns the number of updated rows' do
      count = @connection.exec_update("UPDATE statement_test SET age = 99 WHERE name = 'Alice'")

      expect(count).to eq(1)
    end

    it 'works with bind parameters' do
      binds = [
        ActiveRecord::Relation::QueryAttribute.new('new_age', 50, ActiveRecord::Type::Integer.new),
        ActiveRecord::Relation::QueryAttribute.new('name', 'Alice', ActiveRecord::Type::String.new)
      ]
      count = @connection.exec_update('UPDATE statement_test SET age = ? WHERE name = ?', 'SQL', binds)

      expect(count).to eq(1)

      # Verify the update worked
      result = @connection.internal_exec_query("SELECT age FROM statement_test WHERE name = 'Alice'")
      expect(result.rows.first.first).to eq(50)
    end
  end

  describe 'Rails base class delegation' do
    describe '#exec_query' do
      it 'delegates to internal_exec_query and returns ActiveRecord::Result' do
        result = @connection.exec_query('SELECT * FROM statement_test')

        expect(result).to be_a(ActiveRecord::Result)
        expect(result.rows.length).to eq(3)
      end

      it 'works with bind parameters' do
        bind = ActiveRecord::Relation::QueryAttribute.new('id', 2, ActiveRecord::Type::Integer.new)
        result = @connection.exec_query('SELECT name FROM statement_test WHERE id = ?', 'SQL', [bind])

        expect(result.rows.first).to eq(['Bob'])
      end
    end

    describe '#exec_insert' do
      it 'inserts records and returns the result with RETURNING' do
        result = @connection.exec_insert(
          "INSERT INTO statement_test (id, name, age, active) VALUES (4, 'Diana', 28, true)",
          'SQL',
          [],
          'id'
        )

        expect(result).to be_a(ActiveRecord::Result)
        # RETURNING clause should return the id
        expect(result.rows.first.first).to eq(4)
      end

      it 'works with bind parameters' do
        binds = [
          ActiveRecord::Relation::QueryAttribute.new('id', 5, ActiveRecord::Type::Integer.new),
          ActiveRecord::Relation::QueryAttribute.new('name', 'Eve', ActiveRecord::Type::String.new),
          ActiveRecord::Relation::QueryAttribute.new('age', 32, ActiveRecord::Type::Integer.new),
          ActiveRecord::Relation::QueryAttribute.new('active', true, ActiveRecord::Type::Boolean.new)
        ]
        result = @connection.exec_insert(
          'INSERT INTO statement_test (id, name, age, active) VALUES (?, ?, ?, ?)',
          'SQL',
          binds,
          'id'
        )

        expect(result).to be_a(ActiveRecord::Result)
      end
    end
  end

  describe 'ActiveRecord model integration' do
    let(:model_class) do
      Class.new(ActiveRecord::Base) do
        self.table_name = 'statement_test'

        def self.name
          'StatementTestModel'
        end
      end
    end

    describe 'create operations (exec_insert path)' do
      it 'creates records via ActiveRecord' do
        record = model_class.create!(id: 10, name: 'Test', age: 40, active: true)

        expect(record.id).to eq(10)
        expect(model_class.find(10).name).to eq('Test')
      end
    end

    describe 'read operations (internal_exec_query path)' do
      it 'finds records by id' do
        record = model_class.find(1)

        expect(record.name).to eq('Alice')
      end

      it 'finds records with conditions' do
        records = model_class.where(active: true)

        expect(records.count).to eq(2)
      end

      it 'finds records with parameterized conditions' do
        records = model_class.where('age > ?', 25)

        expect(records.count).to eq(2)
      end
    end

    describe 'update operations (exec_update path)' do
      it 'updates records via ActiveRecord' do
        model_class.where(id: 1).update_all(age: 26)

        expect(model_class.find(1).age).to eq(26)
      end

      it 'updates via model instance' do
        record = model_class.find(1)
        record.update!(age: 27)

        expect(model_class.find(1).age).to eq(27)
      end
    end

    describe 'delete operations (exec_delete path)' do
      it 'deletes records via delete_all' do
        count = model_class.where(active: false).delete_all

        expect(count).to eq(1)
        expect(model_class.count).to eq(2)
      end

      it 'deletes via model instance' do
        record = model_class.find(1)
        record.destroy!

        expect { model_class.find(1) }.to raise_error(ActiveRecord::RecordNotFound)
      end
    end
  end

  describe 'edge cases and error handling' do
    it 'handles queries returning many columns' do
      result = @connection.internal_exec_query('SELECT id, name, age, active FROM statement_test')

      expect(result.columns.length).to eq(4)
    end

    it 'handles queries with aliases' do
      result = @connection.internal_exec_query(
        'SELECT id AS user_id, name AS username FROM statement_test'
      )

      expect(result.columns).to eq(%w[user_id username])
    end

    it 'handles NULL values' do
      @connection.execute("INSERT INTO statement_test VALUES (100, NULL, NULL, NULL)")

      result = @connection.internal_exec_query('SELECT * FROM statement_test WHERE id = 100')

      expect(result.rows.first).to eq([100, nil, nil, nil])
    end

    it 'handles special characters in string values' do
      @connection.execute("INSERT INTO statement_test VALUES (101, 'O''Brien', 45, true)")

      result = @connection.internal_exec_query('SELECT name FROM statement_test WHERE id = 101')

      expect(result.rows.first.first).to eq("O'Brien")
    end

    it 'raises error for invalid SQL' do
      expect {
        @connection.execute('INVALID SQL')
      }.to raise_error(ActiveRecord::StatementInvalid)
    end
  end
end
