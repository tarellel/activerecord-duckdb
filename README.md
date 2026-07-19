# Activerecord::Duckdb

This gem is a DuckDB database adapter for ActiveRecord.

## Description

Activerecord::Duckdb providers DuckDB database access for Ruby on Rails applications.

~ **NOTE:** This gem is still a work in progress, so it might not work exactly as expected just yet. Some ActiveRecord features haven’t been added and/or fully tested.

## Requirements

This gem relies on the [ruby-duckdb](https://github.com/suketa/ruby-duckdb) ruby gem as its database adapter. Thus it provides a seamless integration with the DuckDB database.

Both gems requires that you have [duckdb](https://duckdb.org). DuckDB has many installation options available that can be found on their [installation page](https://duckdb.org/docs/installation/).

```ruby
# OSx
brew install duckdb

# Most Linux distributions
curl https://install.duckdb.org | sh
```

## Installation

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add "activerecord-duckdb"
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install activerecord-duckdb
```

## Usage

### Configuration

Adjust your `database.yml` file to use the duckdb adapter.

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb

test:
  adapter: duckdb
  database: db/test.duckdb

production:
  adapter: duckdb
  database: db/production.duckdb
```

Run some migrations to ensure the database is ready.

```bash
rails g model Notice name:string email:string content:string
```

```ruby
Notice.create(name: 'John Doe', email: 'john@example.com', content: 'Something happened at work today!')
Notice.find_by(email: 'john@example.com')
Notice.all
Notice.last.delete
```

~ At the moment using an in-memory database is very limited and still in development.
**NOTE:** When using a memory database, any transactional operations will be lost when the process exits.
The only reason I can think of is that you might want to use an in-memory database for testing purposes, data analysis, or some sort of quick calculations where the data is not critical.

```yaml
temporary_database:
  adapter: duckdb
  database: :memory
```

```ruby
class User < ApplicationRecord
  establish_connection(:temporary_database)
end
```

Of you can set your own database configuration in the `config/database.yml` file.
When using temporary databases you'll also have to generate your own schema on the fly rather than migrations creating them automatically.

```yml
test:
  adapter: duckdb
  database: :memory

production:
  adapter: duckdb
  database: :memory
```

### Advanced Connection Configuration

The adapter supports advanced configuration options for extensions, settings, secrets, and database attachments. These are configured in your `database.yml` file.

#### Extensions

Install and load DuckDB extensions automatically on connection:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  extensions:
    - httpfs
    - postgres_scanner
    - parquet
```

#### Settings

Configure DuckDB settings. The adapter applies secure defaults which you can override:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  settings:
    threads: 4
    memory_limit: "2GB"
    max_temp_directory_size: "8GB"
```

**Default Settings:**

| Setting                        | Default Value | Description                             |
| ------------------------------ | ------------- | --------------------------------------- |
| `allow_persistent_secrets`     | `false`       | Disable persistent secrets for security |
| `allow_community_extensions`   | `false`       | Disable community extensions            |
| `autoinstall_known_extensions` | `false`       | Disable auto-installing extensions      |
| `autoload_known_extensions`    | `false`       | Disable auto-loading extensions         |
| `threads`                      | `1`           | Number of threads for query execution   |
| `memory_limit`                 | `'1GB'`       | Maximum memory usage                    |
| `max_temp_directory_size`      | `'4GB'`       | Maximum temp directory size             |

**Notes:**

- `allow_persistent_secrets` and `allow_community_extensions` are applied before loading extensions
- `lock_configuration = true` is automatically applied at the end to lock all settings

#### Secrets

Configure secrets for accessing external services (S3, PostgreSQL, etc.). Two styles are supported:

**Style 1: Unnamed secrets** (key is the secret type):

```yaml
development:
  adapter: duckdb
  database: ducklake
  secrets:
    postgres:
      host: localhost
      database: mydb
      user: admin
      password: secret
    s3:
      key_id: AKIAIOSFODNN7EXAMPLE
      secret: wJalrXUtnFEMI/K7MDENG
      region: us-east-1
```

**Style 2: Named secrets** (explicit `type` key, hash key becomes secret name):

```yaml
development:
  adapter: duckdb
  database: ducklake
  secrets:
    my_prod_bucket:
      type: s3
      key_id: AKIAIOSFODNN7EXAMPLE
      secret: wJalrXUtnFEMI/K7MDENG
      region: us-east-1
      scope: "s3://prod-bucket"
    my_dev_bucket:
      type: s3
      key_id: AKIAIOSFODNN7EXAMPLE2
      secret: anotherSecretKey
      region: us-west-2
      scope: "s3://dev-bucket"
```

Named secrets allow multiple secrets of the same type with different scopes.

#### Database Attachments

Attach external databases (PostgreSQL, MySQL, DuckLake, etc.):

```yaml
development:
  adapter: duckdb
  database: ducklake
  extensions:
    - postgres_scanner
    - ducklake
  secrets:
    postgres:
      host: localhost
      database: mydb
      user: admin
      password: secret
  attachments:
    - name: pg_db
      connection_string: "postgres:"
      type: POSTGRES
    - name: ducklake
      connection_string: "ducklake:postgres:"
      options: "DATA_PATH 's3://my-bucket', ENCRYPTED"
```

If you need to switch to a specific attached database after configuration, you can use the `use_database` option:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  attachments:
    - name: analytics
      connection_string: "s3://bucket/analytics.duckdb"
  use_database: analytics # Switch to the attached database
```

#### Remote Server (quack protocol)

DuckDB 1.5.3+ ships the [quack](https://duckdb.org/quack/) core extension, which lets an
embedded DuckDB act as a **client** to a remote DuckDB **server** over the `quack:` protocol
(HTTP, default port `9494`). This adapter can connect to such a server through an optional
`quack:` configuration block.

This feature is **off by default**. When the `quack:` block is absent, the adapter continues to
use a standalone file-based or in-memory database exactly as before.

On the server, start a DuckDB instance serving over quack. You can do this in raw SQL:

```sql
CALL quack_serve('quack:0.0.0.0:9494', token => 'super_secret', allow_other_hostname => true);
```

...or use the launcher this gem provides (see [Running a quack server](#running-a-quack-server)
below).

Then point the adapter at it. The local `database:` acts as the client's control database
(in-memory is typical); the remote server's data is reached through the attached alias:

```yaml
production:
  adapter: duckdb
  database: ":memory:"                       # local client control database
  quack:
    url: "quack:analytics.example.com:9494"   # required: remote server URI
    token: <%= ENV["QUACK_TOKEN"] %>          # optional: auth token
    as: remote                                # optional: ATTACH alias (default: remote)
    use: true                                 # optional: USE the attached db (default: true)
```

The adapter installs and loads the quack extension for you, so you do **not** need to add it to
the `extensions:` list. Under the hood, a populated block emits (in order):

```sql
INSTALL quack;
LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret', SCOPE 'quack:analytics.example.com:9494'); -- only when a token is given
ATTACH 'quack:analytics.example.com:9494' AS remote (TYPE quack);
USE remote;                                                                                  -- unless use: false
```

Configuration options:

| Option  | Required | Default  | Description                                                     |
| ------- | -------- | -------- | --------------------------------------------------------------- |
| `url`   | yes      | —        | Remote server URI, e.g. `"quack:host:9494"`                     |
| `token` | no       | —        | Auth token; registered as a scoped quack `SECRET` when present  |
| `as`    | no       | `remote` | Alias used by `ATTACH ... AS <as>`                              |
| `use`   | no       | `true`   | Whether to `USE` the attached database after attaching          |

Notes:

- A `quack:` block that is empty, or whose keys are all blank, is treated as disabled (no-op).
- A block that provides other keys but omits `url` raises an error rather than producing an
  invalid connection.
- Because quack is a **core** extension, no `allow_community_extensions` relaxation is needed and
  the adapter's secure defaults remain in effect. The explicit `INSTALL` performs a one-time
  network fetch of the extension on first connection.

##### Schema, migrations, and integer primary keys over quack

Ordinary integer (auto-increment) primary keys work over quack — no need to switch your app to
UUIDs. The adapter adapts transparently because a quack `ATTACH` has some hard constraints:

- A column with a function-valued `DEFAULT` (such as `nextval(...)`) breaks `ATTACH`, and the
  client cannot see the server's sequences through the attached catalog.
- `INSERT ... RETURNING`, `UPDATE`, and `DELETE` are not supported directly on an attached quack
  table.

To make Rails work anyway, in quack mode the adapter:

- Creates tables **without** a `DEFAULT nextval()` column default, and creates the backing
  sequence on the server via quack's server-side query channel.
- **Prefetches** the next id from that sequence and includes it in the `INSERT` (so no
  `RETURNING` is needed).
- Routes `UPDATE`/`DELETE` to the server the same way, returning the affected-row count.

**Run your migrations through the quack connection.** Point Rails at the quack server (a
`quack:` block in `database.yml`) and run `rails db:migrate` as usual — `create_table` builds the
schema on the server in the quack-compatible shape. After that, `Model.create`, `find`, `where`,
`update`, and `destroy` all behave normally:

```ruby
User.create!(name: "alice")   # => #<User id: 1, ...>  (id prefetched from the server sequence)
User.where(name: "alice").update_all(active: true)
User.last.destroy
```

##### Running a quack server

The real value of quack is letting **multiple separate processes** (e.g. several Rails/Puma
workers, Sidekiq, and a console) share **one writable** DuckDB database concurrently — something
embedded/in-process DuckDB cannot do because of its single-writer file lock. To get that, run a
**dedicated, long-lived server process** and point every app process at it as a client.

This gem ships a rake task and a `QuackServer` class to launch one:

```bash
# Serve a file-backed database so its data survives restarts
DATABASE=db/shared.duckdb BIND=quack:0.0.0.0:9494 QUACK_TOKEN=super_secret \
  QUACK_ALLOW_OTHER_HOSTNAME=1 bundle exec rake duckdb:quack:serve
```

Environment variables: `DATABASE` (file path or `:memory:`, default `:memory:`), `BIND`
(default `quack:localhost:9494`), `QUACK_TOKEN`, `QUACK_EXTENSIONS` (comma-separated), and
`QUACK_ALLOW_OTHER_HOSTNAME=1` (required to bind a non-localhost address such as `0.0.0.0`).

Or from Ruby:

```ruby
server = ActiveRecord::ConnectionAdapters::Duckdb::QuackServer.new(
  database: 'db/shared.duckdb',
  bind: 'quack:localhost:9494',
  token: ENV['QUACK_TOKEN']
)
server.start # non-blocking; the listener runs in a background thread
server.wait  # keep this process alive (Ctrl-C to stop)
```

Important:

- **Run the server as its own process, not inside your Rails app's connection.** Do not try to
  serve and connect as a client from the *same* process — beyond offering no benefit (you'd be
  routing queries over an HTTP loopback to a database you could query directly), it is unstable
  at process teardown. Server and clients must be **separate processes**.
- Auth tokens must be **at least 4 characters**; `QuackServer` raises early if a shorter one is
  given. If no token is set, the server generates one at startup and (by default) allows all
  queries — set a token for anything beyond local experimentation.
- When binding a public address, front the server with a TLS-terminating reverse proxy (e.g.
  nginx) rather than exposing quack directly, as the DuckDB documentation recommends.

### Sample App setup

The following steps are required to setup a sample application using the `activerecord-duckdb` gem:

1. Create a new Rails application:

```bash
rails new sample_app --database=sqlite3
```

2. Add the `activerecord-duckdb` gem to your Gemfile:

```ruby
gem 'activerecord-duckdb'
```

3. Run `bundle install` to install the gem.

4. Update the `config/database.yml` file to use the `duckdb` adapter:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb

test:
  adapter: duckdb
  database: :memory

production:
  adapter: duckdb
  database: :memory
```

5. Generate a model for the sample application:

```bash
rails g model User name:string email:string
```

5. Run some migrations to ensure the database is ready:

```bash
rails db:create; rails db:migrate
```

6. Create some sample data:

```ruby
User.create(name: 'John Doe', email: 'john@example.com')
User.create(name: 'Jane Doe', email: 'jane@example.com')
```

7. Run some queries:

```ruby
User.all
User.find_by(email: 'john@example.com')
User.last.delete
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tarellel/activerecord-duckdb.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
