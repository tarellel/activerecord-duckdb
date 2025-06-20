# frozen_string_literal: true

# Main entry point for the activerecord-duckdb gem
# Loads the DuckDB adapter and registers it with ActiveRecord
# Provides Rails integration when Rails is present

# Load the adapter regardless of Rails presence
require 'active_record/connection_adapters/duckdb_adapter'

# Register the DuckDB adapter with ActiveRecord
# This works for both Rails and non-Rails environments
# @return [void]
ActiveRecord::ConnectionAdapters.register(
  'duckdb',
  'ActiveRecord::ConnectionAdapters::DuckdbAdapter',
  'active_record/connection_adapters/duckdb_adapter'
)

if defined?(Rails)
  module ActiveRecord
    module ConnectionAdapters
      # Rails integration for the DuckDB adapter
      # Provides rake tasks and ActiveRecord integration when Rails is present
      class DuckdbRailtie < ::Rails::Railtie
        # Registers DuckDB database tasks with Rails
        # @return [void]
        rake_tasks do
          require 'active_record/tasks/duckdb_database_tasks'
        end

        # Sets up DuckDB adapter integration when ActiveRecord loads
        # @return [void]
        ActiveSupport.on_load(:active_record) do
          # Register the database tasks - try multiple approaches for compatibility
          if ActiveRecord::Tasks::DatabaseTasks.respond_to?(:register_task)
            ActiveRecord::Tasks::DatabaseTasks.register_task(
              'duckdb',
              'ActiveRecord::Tasks::DuckdbDatabaseTasks'
            )
          else
            # Fallback for older Rails versions
            ActiveRecord::Tasks::DatabaseTasks.module_eval do
              def self.class_for_adapter(adapter)
                case adapter
                when 'duckdb'
                  ActiveRecord::Tasks::DuckdbDatabaseTasks
                else
                  super
                end
              end
            end
          end
        end
      end
    end
  end
else
  # Non-Rails environment - require the tasks after adapter registration
  require 'active_record/tasks/duckdb_database_tasks'
end
