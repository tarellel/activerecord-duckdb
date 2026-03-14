# Query Execution Call Graph

This document maps the query execution methods in the DuckDB adapter and how they interact across Rails versions.

## Overview

The DuckDB adapter uses **version-specific modules** to integrate with Rails' query execution infrastructure. This approach:
- Minimizes code duplication
- Uses Rails' native implementations where possible
- Maintains compatibility across Rails 7.2, 8.0, and 8.1

## Architecture

```mermaid
flowchart TB
    subgraph "Shared Module: DatabaseStatements"
        execute["execute(sql, name)"]
        cast_result["cast_result(result)"]
        affected_rows["affected_rows(raw_result)"]
    end

    subgraph "Rails 7.2: DatabaseStatementsRails72"
        internal_exec_query_72["internal_exec_query()"]
        exec_delete_72["exec_delete()"]
    end

    subgraph "Rails 8.0+: DatabaseStatementsRails8"
        raw_execute_8["raw_execute()"]
    end

    subgraph "Rails Base Class"
        exec_query_base["exec_query()"]
        exec_delete_base["exec_delete()"]
        internal_exec_query_base["internal_exec_query()"]
    end

    subgraph "DuckDB Driver"
        raw_query["conn.query(sql, *binds)"]
    end

    %% Rails 7.2 flow
    exec_query_base -->|"7.2"| internal_exec_query_72
    internal_exec_query_72 --> raw_query
    internal_exec_query_72 --> cast_result
    exec_delete_72 --> internal_exec_query_72

    %% Rails 8.0+ flow
    exec_query_base -->|"8.0+"| internal_exec_query_base
    internal_exec_query_base --> raw_execute_8
    raw_execute_8 --> raw_query
    internal_exec_query_base --> cast_result
    exec_delete_base -->|"8.0+"| affected_rows
```

## Rails Version Differences

### Rails 7.2

In Rails 7.2, `internal_exec_query` in the base class raises `NotImplementedError`. Adapters **must** implement it.

```mermaid
flowchart TB
    subgraph "Rails 7.2 Base Class"
        exec_query["exec_query()"]
        internal_exec_base["internal_exec_query()<br/>raises NotImplementedError"]
        exec_delete_base["exec_delete()<br/>returns ActiveRecord::Result"]
    end

    subgraph "DuckDB Adapter (DatabaseStatementsRails72)"
        internal_exec_impl["internal_exec_query()"]
        exec_delete_impl["exec_delete()"]
        cast_result["cast_result()"]
    end

    exec_query --> internal_exec_base
    internal_exec_base -.->|"must override"| internal_exec_impl
    internal_exec_impl --> cast_result
    
    exec_delete_base -.->|"override for Integer"| exec_delete_impl
    exec_delete_impl -->|"delegates to"| internal_exec_impl
    exec_delete_impl -->|"extracts count from"| result_rows["result.rows[0][0]"]

    style internal_exec_base fill:#ffcccc
    style exec_delete_base fill:#ffcccc
    style internal_exec_impl fill:#ccffcc
    style exec_delete_impl fill:#ccffcc
```

**Required implementations in `DatabaseStatementsRails72`:**
- `internal_exec_query(sql, name, binds, prepare:, async:, allow_retry:)` → `ActiveRecord::Result`
- `exec_delete(sql, name, binds)` → `Integer` (delegates to `internal_exec_query`, extracts count)

### Rails 8.0 & 8.1

In Rails 8.0+, the base class provides working implementations of `internal_exec_query` and `exec_delete` that delegate to lower-level methods. Adapters implement `raw_execute`, `cast_result`, and `affected_rows`.

```mermaid
flowchart TB
    subgraph "Rails 8.0+ Base Class"
        exec_query["exec_query()"]
        internal_exec_query["internal_exec_query()"]
        internal_execute["internal_execute()"]
        raw_execute_base["raw_execute()<br/>adapter implements"]
        exec_delete["exec_delete()"]
        affected_rows_call["affected_rows()"]
        cast_result_call["cast_result()"]
    end

    subgraph "DuckDB Adapter (DatabaseStatementsRails8)"
        raw_execute_impl["raw_execute()"]
    end

    subgraph "DuckDB Adapter (Shared)"
        cast_result_impl["cast_result()"]
        affected_rows_impl["affected_rows()"]
    end

    exec_query --> internal_exec_query
    internal_exec_query --> internal_execute
    internal_execute --> raw_execute_base
    raw_execute_base -.->|"implemented by"| raw_execute_impl
    internal_exec_query --> cast_result_call
    cast_result_call -.->|"implemented by"| cast_result_impl

    exec_delete --> internal_execute
    exec_delete --> affected_rows_call
    affected_rows_call -.->|"implemented by"| affected_rows_impl

    style raw_execute_impl fill:#ccffcc
    style cast_result_impl fill:#ccffcc
    style affected_rows_impl fill:#ccffcc
```

**Required implementations for Rails 8.0+:**
- `raw_execute(sql, name, binds, prepare:, async:, allow_retry:, materialize_transactions:, batch:)` → `DuckDB::Result`
- `cast_result(raw_result)` → `ActiveRecord::Result` (shared)
- `affected_rows(raw_result)` → `Integer` (shared)

## File Structure

```
lib/active_record/connection_adapters/duckdb/
├── database_statements.rb          # Shared: execute, cast_result, affected_rows
├── database_statements_rails72.rb  # Rails 7.2: internal_exec_query, exec_delete
└── database_statements_rails8.rb   # Rails 8.0+: raw_execute
```

**Conditional inclusion in `duckdb_adapter.rb`:**

```ruby
include Duckdb::DatabaseStatements

if ActiveRecord::VERSION::MAJOR >= 8
  require 'active_record/connection_adapters/duckdb/database_statements_rails8'
  include Duckdb::DatabaseStatementsRails8
else
  require 'active_record/connection_adapters/duckdb/database_statements_rails72'
  include Duckdb::DatabaseStatementsRails72
end
```

## Method Signatures

### Shared (all Rails versions)

```ruby
# Raw SQL execution - returns native DuckDB result
def execute(sql, name = nil)
  # → DuckDB::Result
end

# Convert DuckDB result to ActiveRecord result
def cast_result(result)
  # → ActiveRecord::Result
end

# Extract row count from raw result
def affected_rows(raw_result)
  # → Integer (raw_result.rows_changed)
end
```

### Rails 7.2 Specific

```ruby
# Query execution - required since base class raises NotImplementedError
def internal_exec_query(sql, name = 'SQL', binds = [],
                        prepare: false, async: false, allow_retry: false)
  # → ActiveRecord::Result (via cast_result)
end

# Delete/Update - extracts count from DuckDB's result set
def exec_delete(sql, name = nil, binds = [])
  result = internal_exec_query(sql, name, binds)
  result.rows.first&.first || 0  # DuckDB returns [[count]]
end
alias exec_update exec_delete
```

### Rails 8.0+ Specific

```ruby
# Raw execution - base class internal_exec_query delegates here
def raw_execute(sql, name = nil, binds = [],
                prepare: false, async: false, allow_retry: false,
                materialize_transactions: true, batch: false)
  casted_binds = type_casted_binds(binds)
  log(sql, name, binds, casted_binds, async: async) do
    with_raw_connection(allow_retry:, materialize_transactions:) do |conn|
      casted_binds.empty? ? conn.query(sql) : conn.query(sql, *casted_binds)
    end
  end
  # → DuckDB::Result (base class wraps with cast_result)
end
```

## Query Flow Examples

### SELECT Query

```mermaid
sequenceDiagram
    participant App as Application
    participant AR as ActiveRecord
    participant Base as Rails Base
    participant Adapter as DuckDB Adapter
    participant DB as DuckDB

    App->>AR: User.find(1)
    AR->>Base: exec_query(sql, binds)
    
    alt Rails 7.2
        Base->>Adapter: internal_exec_query()
        Adapter->>DB: conn.query(sql, *binds)
        DB-->>Adapter: DuckDB::Result
        Adapter->>Adapter: cast_result()
        Adapter-->>Base: ActiveRecord::Result
    else Rails 8.0+
        Base->>Base: internal_exec_query()
        Base->>Adapter: raw_execute()
        Adapter->>DB: conn.query(sql, *binds)
        DB-->>Adapter: DuckDB::Result
        Adapter-->>Base: DuckDB::Result
        Base->>Adapter: cast_result()
        Adapter-->>Base: ActiveRecord::Result
    end
    
    Base-->>AR: ActiveRecord::Result
    AR-->>App: User instance
```

### DELETE Query

```mermaid
sequenceDiagram
    participant App as Application
    participant AR as ActiveRecord
    participant Base as Rails Base
    participant Adapter as DuckDB Adapter
    participant DB as DuckDB

    App->>AR: User.delete_all
    AR->>Base: exec_delete(sql, binds)
    
    alt Rails 7.2
        Base->>Adapter: exec_delete()
        Adapter->>Adapter: internal_exec_query()
        Adapter->>DB: conn.query(sql, *binds)
        DB-->>Adapter: DuckDB::Result with [[count]]
        Adapter->>Adapter: cast_result()
        Adapter->>Adapter: result.rows[0][0]
        Adapter-->>Base: Integer
    else Rails 8.0+
        Base->>Base: internal_execute()
        Base->>Adapter: raw_execute()
        Adapter->>DB: conn.query(sql, *binds)
        DB-->>Adapter: DuckDB::Result
        Adapter-->>Base: DuckDB::Result
        Base->>Adapter: affected_rows()
        Adapter-->>Base: Integer (rows_changed)
    end
    
    Base-->>AR: Integer
    AR-->>App: rows deleted count
```

## Design Decisions

### 1. Version-Specific Modules Over Shadowing

**Before:** Single `internal_exec_query` that shadowed Rails 8's base implementation.

**After:** Separate modules for each Rails version:
- Rails 7.2: Implements `internal_exec_query` (required)
- Rails 8.0+: Implements `raw_execute` (lets base handle higher-level methods)

**Benefits:**
- Uses Rails' native query infrastructure for logging, retries, async
- Cleaner separation of concerns
- Easier to adapt to future Rails changes

### 2. DuckDB DELETE Returns Count in Result Set

DuckDB's DELETE/UPDATE statements return the affected row count as a result set:

```sql
DELETE FROM users WHERE id = 1;
-- Returns: columns=["Count"], rows=[[1]]
```

Also available via `result.rows_changed`.

**Rails 7.2:** Extract from `result.rows[0][0]` after `internal_exec_query`
**Rails 8.0+:** Use `affected_rows(result)` → `result.rows_changed`

### 3. Shared vs Version-Specific Methods

| Method | Location | Reason |
|--------|----------|--------|
| `execute` | Shared | Same across all versions |
| `cast_result` | Shared | Used by all query paths |
| `affected_rows` | Shared | Rails 8.0+ base class calls it |
| `internal_exec_query` | Rails 7.2 only | Base class provides in 8.0+ |
| `exec_delete` | Rails 7.2 only | Base class handles in 8.0+ |
| `raw_execute` | Rails 8.0+ only | New API in Rails 8 |

---

## Schema Statements

The `SchemaStatements` module also uses version-specific modules due to a breaking change in Rails 8.1's `Column` class constructor.

### Column Constructor Change (Rails 8.1)

```mermaid
flowchart TB
    subgraph "Rails 7.2 / 8.0"
        new_col_80["new_column_from_field()"]
        col_ctor_80["Column.new(name, default, sql_type_metadata, null, default_function, ...)"]
    end

    subgraph "Rails 8.1+"
        new_col_81["new_column_from_field()"]
        col_ctor_81["Column.new(name, cast_type, default, sql_type_metadata, null, default_function, ...)"]
        lookup_cast["lookup_cast_type_from_column()"]
    end

    new_col_80 --> col_ctor_80
    new_col_81 --> lookup_cast
    lookup_cast --> col_ctor_81
```

### Schema File Structure

```
lib/active_record/connection_adapters/duckdb/
├── schema_statements.rb            # Shared: tables, indexes, create_table, etc.
├── schema_statements_rails80.rb    # Rails 7.2/8.0: new_column_from_field
└── schema_statements_rails81.rb    # Rails 8.1+: new_column_from_field (with cast_type)
```

### Module Inclusion

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

### Schema Methods Overview

| Method | Location | Purpose |
|--------|----------|---------|
| `tables` | Shared | List all tables |
| `indexes` | Shared | List indexes for a table |
| `create_table` | Shared | Create table with sequence handling |
| `create_sequence` | Shared | Create DuckDB sequence |
| `type_to_sql` | Shared | Convert Rails types to DuckDB SQL |
| `new_column_from_field` | Version-specific | Create Column from DB metadata |
| `fetch_type_metadata` | Shared | Parse DuckDB type strings |
