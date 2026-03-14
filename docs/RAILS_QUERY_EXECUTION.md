# Rails Query Execution Architecture

This document summarizes how query execution works across supported Rails versions (7.2, 8.0, 8.1) and how the DuckDB adapter integrates with it.

## Query Execution Call Structure

### Rails 7.2

```
Model.find_by(...) / Model.create(...)
  └── select_all / insert
        └── internal_exec_query(sql, name, binds, prepare:, async:, allow_retry:)
              └── [Adapter must implement - base class raises NotImplementedError]
```

**Key point:** Adapters MUST implement `internal_exec_query` in Rails 7.2.

### Rails 8.0 / 8.1

```
Model.find_by(...) / Model.create(...)
  └── select_all / insert
        └── internal_exec_query(...)  # Default implementation provided
              └── cast_result(internal_execute(...))
                    └── raw_execute(...)
                          └── perform_query(raw_connection, sql, binds, ...)
                                └── [Adapter should implement]
```

**Key point:** Rails 8.0+ provides a default `internal_exec_query` that delegates to `perform_query`. However, adapters can still override `internal_exec_query` directly.

## DuckDB Adapter Implementation

The adapter uses **version-specific modules** to integrate optimally with each Rails version:

| Rails Version | Query Execution | Schema Statements |
|--------------|-----------------|-------------------|
| 7.2 | `DatabaseStatementsRails72` | `SchemaStatementsRails80` |
| 8.0 | `DatabaseStatementsRails8` | `SchemaStatementsRails80` |
| 8.1 | `DatabaseStatementsRails8` | `SchemaStatementsRails81` |

### Query Execution Strategy

- **Rails 7.2**: Implements `internal_exec_query` (required - base class raises `NotImplementedError`)
- **Rails 8.0+**: Implements `raw_execute`, lets base class handle `internal_exec_query`

This allows Rails 8.x to use its native query infrastructure (logging, retries, async support).

### Module Loading

```ruby
# In duckdb_adapter.rb
if ActiveRecord::VERSION::MAJOR >= 8
  require 'active_record/connection_adapters/duckdb/database_statements_rails8'
  include Duckdb::DatabaseStatementsRails8
else
  require 'active_record/connection_adapters/duckdb/database_statements_rails72'
  include Duckdb::DatabaseStatementsRails72
end
```

## Column Class Signature Change (Rails 8.1)

Rails 8.1 introduced a breaking change to the `Column` class:

```ruby
# Rails 7.2 / 8.0
def initialize(name, default, sql_type_metadata, null, default_function, ...)

# Rails 8.1+
def initialize(name, cast_type, default, sql_type_metadata, null, default_function, ...)
```

The adapter handles this with version-specific schema statement modules:

```ruby
# In duckdb_adapter.rb
if ActiveRecord::VERSION::MAJOR > 8 ||
   (ActiveRecord::VERSION::MAJOR == 8 && ActiveRecord::VERSION::MINOR >= 1)
  require 'active_record/connection_adapters/duckdb/schema_statements_rails81'
  include Duckdb::SchemaStatementsRails81
else
  require 'active_record/connection_adapters/duckdb/schema_statements_rails80'
  include Duckdb::SchemaStatementsRails80
end
```

Each module implements `new_column_from_field` with the correct Column constructor signature.

## Testing with Appraisal

Run tests against all Rails versions:

```bash
# All versions
bundle exec appraisal rspec

# Specific version
bundle exec appraisal rails-7.2 rspec
bundle exec appraisal rails-8.0 rspec
bundle exec appraisal rails-8.1 rspec
```

## Related Methods

| Method | Purpose | Module |
|--------|---------|--------|
| `internal_exec_query` | Execute query, return ActiveRecord::Result | Rails 7.2 only |
| `raw_execute` | Low-level query execution | Rails 8.0+ only |
| `execute` | Direct SQL execution, returns raw DuckDB result | Shared |
| `cast_result` | Convert DuckDB result to ActiveRecord::Result | Shared |
| `affected_rows` | Get row count from raw result | Shared |
| `new_column_from_field` | Create Column object from DB field info | Rails 8.0 / 8.1 |

## File Structure

```
lib/active_record/connection_adapters/duckdb/
├── database_statements.rb          # Shared: execute, cast_result, affected_rows
├── database_statements_rails72.rb  # Rails 7.2: internal_exec_query, exec_delete
├── database_statements_rails8.rb   # Rails 8.0+: raw_execute
├── schema_statements.rb            # Shared schema operations
├── schema_statements_rails80.rb    # Rails 7.2/8.0: new_column_from_field
└── schema_statements_rails81.rb    # Rails 8.1+: new_column_from_field (with cast_type)
```

## Future Considerations

1. **`write_query?` implementation** - Currently returns `false`. Should probably return `true` for INSERT/UPDATE/DELETE to properly invalidate caches.

2. **SQLite3 adapter reference** - The SQLite3 adapter in Rails is a good reference implementation for in-process database adapters.
