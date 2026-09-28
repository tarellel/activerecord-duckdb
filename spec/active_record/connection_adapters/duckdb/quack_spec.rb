# frozen_string_literal: true

require 'spec_helper'

# Specs for the optional quack (client/server) remote protocol configuration.
#
# quack requires a live remote DuckDB server, which isn't available in the test
# suite, so these specs exercise `configure_quack` in isolation: they mock the
# raw connection and assert on the exact SQL emitted (and on the guard/validation
# behavior) rather than connecting to a real server. When a server is available,
# the emitted SQL should be verified end-to-end against DuckDB >= 1.5.3.
RSpec.describe 'DuckDB quack remote protocol configuration' do
  # Builds an allocated adapter with the given config and a fake raw connection
  # that records every SQL statement executed, then runs configure_quack.
  #
  # @param quack [Object] the value for the :quack config key (or :none to omit it)
  # @return [Array<String>] SQL statements executed, in order
  def executed_sql_for(quack: :none)
    adapter = ActiveRecord::ConnectionAdapters::DuckdbAdapter.allocate
    config = { database: ':memory:' }
    config[:quack] = quack unless quack == :none
    adapter.instance_variable_set(:@config, config)

    executed = []
    raw = instance_double(DuckDB::Connection)
    allow(raw).to receive(:execute) { |sql| executed << sql }
    allow(adapter).to receive(:raw_connection).and_return(raw)

    adapter.send(:configure_quack)
    executed
  end

  context 'when the quack block is absent or blank (disabled)' do
    it 'does nothing when there is no quack key' do
      expect(executed_sql_for).to be_empty
    end

    it 'does nothing when the quack block is nil (empty YAML key)' do
      expect(executed_sql_for(quack: nil)).to be_empty
    end

    it 'does nothing when the quack block is an empty hash' do
      expect(executed_sql_for(quack: {})).to be_empty
    end

    it 'does nothing when every key is blank (valueless YAML keys)' do
      expect(executed_sql_for(quack: { url: nil, token: '', as: nil })).to be_empty
    end
  end

  context 'when the quack block is present but invalid' do
    it 'raises when url is missing but other keys are given' do
      expect { executed_sql_for(quack: { token: 'abc' }) }
        .to raise_error(ArgumentError, /missing a `url`/)
    end

    it 'raises when url is blank but other keys are given' do
      expect { executed_sql_for(quack: { url: '', token: 'abc' }) }
        .to raise_error(ArgumentError, /missing a `url`/)
    end
  end

  context 'with a minimal valid block (url only)' do
    subject(:sql) { executed_sql_for(quack: { url: 'quack:localhost:9494' }) }

    it 'installs and loads the quack extension itself' do
      expect(sql).to start_with('INSTALL quack', 'LOAD quack')
    end

    it 'does not create a secret when no token is given' do
      expect(sql).not_to include(a_string_matching(/CREATE SECRET/))
    end

    it 'attaches the remote database with the default alias and TYPE quack' do
      expect(sql).to include("ATTACH 'quack:localhost:9494' AS remote (TYPE quack)")
    end

    it 'switches to the attached database by default' do
      expect(sql).to include('USE remote')
    end
  end

  context 'with a token' do
    subject(:sql) do
      executed_sql_for(quack: { url: 'quack:localhost:9494', token: 'super_secret' })
    end

    it 'creates a scoped quack secret before attaching' do
      secret = "CREATE SECRET (TYPE quack, TOKEN 'super_secret', SCOPE 'quack:localhost:9494')"
      attach = "ATTACH 'quack:localhost:9494' AS remote (TYPE quack)"
      expect(sql.index(secret)).to be < sql.index(attach)
    end
  end

  context 'with a custom alias' do
    subject(:sql) do
      executed_sql_for(quack: { url: 'quack:localhost:9494', as: 'warehouse' })
    end

    it 'attaches under the given alias' do
      expect(sql).to include("ATTACH 'quack:localhost:9494' AS warehouse (TYPE quack)")
    end

    it 'switches to the given alias' do
      expect(sql).to include('USE warehouse')
    end
  end

  context 'when use is false' do
    subject(:sql) do
      executed_sql_for(quack: { url: 'quack:localhost:9494', use: false })
    end

    it 'attaches the remote database' do
      expect(sql).to include("ATTACH 'quack:localhost:9494' AS remote (TYPE quack)")
    end

    it 'does not switch the active database' do
      expect(sql).not_to include(a_string_matching(/\AUSE /))
    end
  end

  context 'with string keys (as parsed from database.yml)' do
    subject(:sql) do
      executed_sql_for(quack: { 'url' => 'quack:localhost:9494', 'token' => 'super_secret' })
    end

    it 'normalizes string keys and emits the expected statements' do
      expect(sql).to include(
        "CREATE SECRET (TYPE quack, TOKEN 'super_secret', SCOPE 'quack:localhost:9494')",
        "ATTACH 'quack:localhost:9494' AS remote (TYPE quack)",
        'USE remote'
      )
    end
  end

  it 'escapes single quotes in the token to prevent broken/injected SQL' do
    sql = executed_sql_for(quack: { url: 'quack:localhost:9494', token: "a'b" })
    expect(sql).to include("CREATE SECRET (TYPE quack, TOKEN 'a''b', SCOPE 'quack:localhost:9494')")
  end
end
