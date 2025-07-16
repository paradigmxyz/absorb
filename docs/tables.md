
## Tables
- tables are defined using `Table` classes
- each `Table` class can represent multiple table instances through use of table parameters
- each table instance is a `Table` class combined with a set of values for the table parameters
- the name of each table instance is derived from the `Table` class and the parameters using the `Table.name_template`

### Table Storage
- each table has a directory
- contents:
    - table_metadata.json:
        - table_version
        - table_parameters
        - table_recipe
        - update_schedule
    - parquet files: store the table content
    - collection.log

### Table Names
- data source name
    - example: `defillama`
- table class name
    - example: `ChainFees`
- table class snake name
    - example: `chain_fees`
- cli table recipe name
    - example: `defillama.chain_fees_top_{n}, defillama.chain_fees`
    - this is a list of name template that can be used to make new names
- table name
    - example: `chain_fees` or `chain_fees_top_100`
- table full name
    - example: `defillama.chain_fees` or `defillama.chain_fees_top_100`
    - source + table_name

### Table Cardinality
    - each source have have multiple Table classes
    - each Table class can have one table recipe
    - each table recipe can have multiple templates
    - each Table class have multiple table instances
    - each table instance has one short name and one full name
    - not every table class snake name is also a table name, depending on templates
    - not every variable must appear in a table name
        - this means in theory different parameter sets could have the same table name
        - this means that a table name is not always sufficient to identify a table instance

### Table representations
- table recipes
    - `Table` class
    - table class snake name
- specific tables
    - `Table` instance
    - `TableDict`
    - table name
    - `Table` directory on disk

### Table operations
- recipe --> specific
    - convert a (recipe, parameters) pair into a specific table
    - get all specific tables created by a specific recipe
- specific --> specific
    - convert a table name to a `Table` instance (not always possible)
- specific --> recipe
    - get table recipe of specific table

### Table groups
- a `TableGroup` is a collection of specific tables
- a `TableGroup` can be used to collect multiple tables together

