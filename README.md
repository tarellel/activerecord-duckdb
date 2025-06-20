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
  database: db/development.db

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
