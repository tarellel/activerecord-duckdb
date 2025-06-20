# TODO:

- Implement support DuckDB's UUID type for primary keys
  - At the moment we use nextval because we always assume it will be a sequence Integer ID
  - https://duckdb.org/docs/stable/sql/functions/utility#gen_random_uuid
- Add support for additional Data Types
  - Blob
    - https://duckdb.org/docs/stable/sql/data_types/blob
  - Enum
    - https://duckdb.org/docs/stable/sql/data_types/enum
  - List
    - https://duckdb.org/docs/stable/sql/data_types/list
  - Map
    - https://duckdb.org/docs/stable/sql/data_types/map
  - Struct
    - https://duckdb.org/docs/stable/sql/data_types/struct
