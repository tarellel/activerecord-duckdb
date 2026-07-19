## [Unreleased]

- Add optional support for connecting to a remote DuckDB server via the quack protocol (`quack:` config block, off by default)

## [0.1.1] - 2026-07-08

- Cleanup code so all linters pass
- Adjust tests for better test coverage
- Ensure Duckdb is using the ActiveRecord timezone rather than a monkey patch
- Cleanup and fix tests for all specs are passing
- Update github action for installing duckdb
- Change minimum RUBY_VERSION to require >= 3.3
- Update gems versions
- Cleanup minor deadcode and nil returns
- Minor touchup to README.md contents
- Add support for DuckDB ducklake (#5 - buenaventure)
- Add support for DuckDB extensions (#5 - buenaventure)

## [0.1.0] - 2025-06-18

- Initial release
