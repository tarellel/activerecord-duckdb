# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # Launches a DuckDB instance as a quack server so that multiple *separate*
      # client processes can share one database with concurrent reads and writes.
      #
      # This is deliberately decoupled from the per-connection client configuration
      # (see +DuckdbAdapter#configure_quack+). A quack server is long-lived shared
      # infrastructure meant to run as its own process; it is *not* something to spin
      # up inside a pooled ActiveRecord connection (each pooled connection would try to
      # bind the same port and would serve its own separate database).
      #
      # The server opens the database file and serves it over the +quack:+ protocol;
      # clients then connect to the server (see the +quack:+ config block) rather than
      # opening the file directly. This is what lets multiple processes write to one
      # DuckDB database, which embedded/in-process DuckDB cannot do on its own.
      #
      # @example Start a server from Ruby
      #   server = ActiveRecord::ConnectionAdapters::Duckdb::QuackServer.new(
      #     database: 'db/shared.duckdb',
      #     bind: 'quack:localhost:9494',
      #     token: ENV['QUACK_TOKEN']
      #   )
      #   server.start   # non-blocking: the listener runs in a background thread
      #   server.wait    # block the current process to keep the listener alive
      class QuackServer
        # Default bind URI: localhost on the standard quack port.
        DEFAULT_BIND = 'quack:localhost:9494'

        # Minimum token length enforced by the quack server.
        MIN_TOKEN_LENGTH = 4

        # @return [String] the database opened and served (file path or ':memory:')
        attr_reader :database
        # @return [String] the quack bind URI, e.g. "quack:localhost:9494"
        attr_reader :bind
        # @return [String, nil] the auth token clients must present, if any
        attr_reader :token
        # @return [Array<String>] extra extensions to INSTALL/LOAD before serving
        attr_reader :extensions
        # @return [Boolean] whether to allow binding a non-local hostname
        attr_reader :allow_other_hostname
        # @return [DuckDB::Connection, nil] the serving connection once started
        attr_reader :connection

        # @param database [String] file path to serve, or ':memory:' (default)
        # @param bind [String] quack bind URI (default: {DEFAULT_BIND})
        # @param token [String, nil] auth token required from clients
        # @param extensions [Array<String>] extra extensions to load before serving
        # @param allow_other_hostname [Boolean] pass allow_other_hostname to quack_serve
        #   (required when binding a non-localhost address such as 0.0.0.0)
        def initialize(database: ':memory:', bind: DEFAULT_BIND, token: nil,
                       extensions: [], allow_other_hostname: false)
          @database = database.to_s.empty? ? ':memory:' : database.to_s
          @bind = bind.presence || DEFAULT_BIND
          @token = token.presence
          @extensions = Array(extensions).compact
          @allow_other_hostname = allow_other_hostname
          @connection = nil

          return unless @token && @token.length < MIN_TOKEN_LENGTH

          raise ArgumentError,
                "quack server token must be at least #{MIN_TOKEN_LENGTH} characters long"
        end

        # Builds the SQL statements executed on startup, in order.
        # Exposed separately so it can be asserted in tests without a live server.
        # @return [Array<String>] SQL statements
        def startup_sql
          sql = ['INSTALL quack', 'LOAD quack']
          extensions.each do |extension|
            sql << "INSTALL #{extension}"
            sql << "LOAD #{extension}"
          end
          sql << serve_sql
          sql
        end

        # Builds the CALL quack_serve(...) statement.
        # @return [String] the serve SQL
        def serve_sql
          params = []
          params << "token => #{quote(token)}" if token
          params << 'allow_other_hostname => true' if allow_other_hostname

          serve = "CALL quack_serve(#{quote(bind)}"
          serve << ", #{params.join(", ")}" unless params.empty?
          serve << ')'
          serve
        end

        # Opens the database, loads quack, and begins serving. Non-blocking: the
        # quack listener runs in a background thread, so this returns once serving
        # has started. Use {#wait} to keep the owning process alive.
        # @return [self]
        def start
          db = memory? ? DuckDB::Database.open : DuckDB::Database.open(database)
          @connection = db.connect
          startup_sql.each { |statement| @connection.execute(statement) }
          self
        end

        # Blocks the current thread indefinitely to keep the background listener
        # alive. Returns when interrupted (e.g. Ctrl-C).
        # @return [void]
        def wait
          sleep
        rescue Interrupt
          nil
        end

        # Stops serving and closes the connection.
        # @return [void]
        def stop
          @connection&.close
          @connection = nil
        end

        private

        # @return [Boolean] whether the served database is in-memory
        def memory?
          DuckdbAdapter::MEMORY_MODE_KEYS.include?(database)
        end

        # Single-quotes and escapes a value for inline SQL, matching the adapter's
        # quoting of string literals.
        # @param value [Object] the value to quote
        # @return [String] the quoted literal
        def quote(value)
          "'#{value.to_s.gsub("'", "''")}'"
        end
      end
    end
  end
end
