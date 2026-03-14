# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Transactions and Savepoints' do
  describe 'transaction methods' do
    let(:connection) do
      ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
      ActiveRecord::Base.connection
    end

    before do
      connection.execute('CREATE TABLE tx_test (id INTEGER, name VARCHAR)')
    end

    after do
      connection.execute('DROP TABLE IF EXISTS tx_test') rescue nil
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
    end

    describe '#begin_db_transaction' do
      it 'starts a database transaction' do
        connection.begin_db_transaction
        connection.execute("INSERT INTO tx_test VALUES (1, 'TxTest')")
        connection.commit_db_transaction

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id = 1')
        expect(result.first.first).to eq(1)
      end
    end

    describe '#commit_db_transaction' do
      it 'commits changes to the database' do
        connection.begin_db_transaction
        connection.execute("INSERT INTO tx_test VALUES (2, 'Committed')")
        connection.commit_db_transaction

        result = connection.execute('SELECT name FROM tx_test WHERE id = 2')
        expect(result.first.first).to eq('Committed')
      end
    end

    describe '#exec_rollback_db_transaction' do
      it 'rolls back changes to the database' do
        connection.begin_db_transaction
        connection.execute("INSERT INTO tx_test VALUES (3, 'Rollback')")
        connection.exec_rollback_db_transaction

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id = 3')
        expect(result.first.first).to eq(0)
      end

      it 'discards all changes since begin' do
        connection.execute("INSERT INTO tx_test VALUES (100, 'Original')")

        connection.begin_db_transaction
        connection.execute("INSERT INTO tx_test VALUES (4, 'First')")
        connection.execute("INSERT INTO tx_test VALUES (5, 'Second')")
        connection.execute("UPDATE tx_test SET name = 'Modified' WHERE id = 100")
        connection.exec_rollback_db_transaction

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id IN (4, 5)')
        expect(result.first.first).to eq(0)

        # Original data should be unchanged
        result = connection.execute('SELECT name FROM tx_test WHERE id = 100')
        expect(result.first.first).to eq('Original')
      end
    end

    describe 'Rails transaction block' do
      it 'commits on successful block completion' do
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO tx_test VALUES (10, 'BlockTest')")
        end

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id = 10')
        expect(result.first.first).to eq(1)
      end

      it 'rolls back on exception' do
        expect do
          ActiveRecord::Base.transaction do
            connection.execute("INSERT INTO tx_test VALUES (11, 'WillRollback')")
            raise StandardError, 'Intentional error'
          end
        end.to raise_error(StandardError)

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id = 11')
        expect(result.first.first).to eq(0)
      end

      it 'rolls back on ActiveRecord::Rollback' do
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO tx_test VALUES (12, 'Rollback')")
          raise ActiveRecord::Rollback
        end

        result = connection.execute('SELECT COUNT(*) FROM tx_test WHERE id = 12')
        expect(result.first.first).to eq(0)
      end
    end
  end

  describe 'savepoints' do
    let(:savepoint_error) { ActiveRecord::ConnectionAdapters::SavepointsNotSupported }

    it 'does not support savepoints' do
      with_memory_connection do |conn|
        expect(conn.supports_savepoints?).to be false
      end
    end

    it 'create_savepoint raises SavepointsNotSupported' do
      with_memory_connection do |conn|
        expect { conn.create_savepoint('test_savepoint') }.to raise_error(savepoint_error)
        expect { conn.create_savepoint }.to raise_error(savepoint_error)
      end
    end

    it 'exec_rollback_to_savepoint raises SavepointsNotSupported' do
      with_memory_connection do |conn|
        expect { conn.exec_rollback_to_savepoint('test_savepoint') }.to raise_error(savepoint_error)
        expect { conn.exec_rollback_to_savepoint }.to raise_error(savepoint_error)
      end
    end

    it 'release_savepoint raises SavepointsNotSupported' do
      with_memory_connection do |conn|
        expect { conn.release_savepoint('test_savepoint') }.to raise_error(savepoint_error)
        expect { conn.release_savepoint }.to raise_error(savepoint_error)
      end
    end
  end

  describe 'nested transactions with requires_new (Rails API integration)' do
    # Rails uses savepoints for transaction(requires_new: true)
    # Since DuckDB doesn't support savepoints, this raises NotImplementedError

    let(:connection) do
      ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')
      ActiveRecord::Base.connection
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
    end

    it 'nested transaction with requires_new raises SavepointsNotSupported' do
      connection.execute('CREATE TABLE nested_test (id INTEGER, name VARCHAR)')

      expect do
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO nested_test VALUES (1, 'outer')")
          ActiveRecord::Base.transaction(requires_new: true) do
            connection.execute("INSERT INTO nested_test VALUES (2, 'inner')")
          end
        end
      end.to raise_error(ActiveRecord::ConnectionAdapters::SavepointsNotSupported)
    end

    it 'nested transaction with requires_new rolls back outer transaction on error' do
      connection.execute('CREATE TABLE rollback_test (id INTEGER, name VARCHAR)')

      expect do
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO rollback_test VALUES (1, 'outer')")
          ActiveRecord::Base.transaction(requires_new: true) do
            connection.execute("INSERT INTO rollback_test VALUES (2, 'inner')")
          end
        end
      end.to raise_error(ActiveRecord::ConnectionAdapters::SavepointsNotSupported)

      # The outer record should also be rolled back since the exception propagates
      result = connection.execute('SELECT COUNT(*) FROM rollback_test')
      expect(result.first.first).to eq(0)
    end

    it 'nested transaction without requires_new joins outer transaction' do
      connection.execute('CREATE TABLE joined_test (id INTEGER, name VARCHAR)')

      ActiveRecord::Base.transaction do
        connection.execute("INSERT INTO joined_test VALUES (1, 'outer')")
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO joined_test VALUES (2, 'inner')")
        end
      end

      result = connection.execute('SELECT COUNT(*) FROM joined_test')
      expect(result.first.first).to eq(2)
    end

    it 'exception in joined nested transaction rolls back entire transaction' do
      connection.execute('CREATE TABLE exception_test (id INTEGER, name VARCHAR)')

      expect do
        ActiveRecord::Base.transaction do
          connection.execute("INSERT INTO exception_test VALUES (1, 'outer')")
          ActiveRecord::Base.transaction do
            connection.execute("INSERT INTO exception_test VALUES (2, 'inner')")
            raise StandardError, 'Intentional error'
          end
        end
      end.to raise_error(StandardError, 'Intentional error')

      # Both records should be rolled back
      result = connection.execute('SELECT COUNT(*) FROM exception_test')
      expect(result.first.first).to eq(0)
    end
  end
end
