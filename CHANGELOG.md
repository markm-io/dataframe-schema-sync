# Changelog

## v1.0.0 (2025-05-26)

## v0.21.6 (2025-03-28)

### Bug fixes

- Remove logging statements and error handling for json and datetime parsing ([`3ff9742`](https://github.com/markm-io/dataframe-schema-sync/commit/3ff9742194532981afaf571aa96b34a9033eec03))

## v0.21.5 (2025-03-15)

### Bug fixes

- Replace empty strings with na values in schema inference before dropping columns ([`2c59972`](https://github.com/markm-io/dataframe-schema-sync/commit/2c599725dce917db63d934f60738760ed71257ea))

## v0.21.4 (2025-03-14)

### Bug fixes

- Simplify boolean value detection by removing redundant unique value checks ([`62b6572`](https://github.com/markm-io/dataframe-schema-sync/commit/62b657240a9ff7305539201d8efd671f0865ffb0))

## v0.21.3 (2025-03-14)

### Bug fixes

- Drop columns with all nan values in schema inference before cleaning names ([`6e69cba`](https://github.com/markm-io/dataframe-schema-sync/commit/6e69cba7d6ab1410a7dda0677530adbb324d13fe))

## v0.21.2 (2025-03-14)

### Bug fixes

- Drop columns with all nan values before cleaning names in dataframe ([`936b98c`](https://github.com/markm-io/dataframe-schema-sync/commit/936b98c1ef1df454c58d2402128803f12bc6b48e))

## v0.21.1 (2025-03-14)

### Bug fixes

- Enhance clean_dataframe_names to support cleaning index names for both regular and multiindex dataframes ([`49233e8`](https://github.com/markm-io/dataframe-schema-sync/commit/49233e82c8f068803a80690391637e672ebd8fdb))

## v0.21.0 (2025-03-14)

### Features

- Update clean_dataframe_names call to use schemainference class for column name cleaning ([`a441330`](https://github.com/markm-io/dataframe-schema-sync/commit/a4413309e20e1401d9f48d7a1c775bf19ca9465f))

## v0.20.0 (2025-03-14)

### Features

- Enhance clean_dataframe_names to preserve original index during column name cleaning ([`de48966`](https://github.com/markm-io/dataframe-schema-sync/commit/de48966ee2c841bacd596a98ecb95cf83b9e85d5))

## v0.19.1 (2025-03-14)

### Bug fixes

- Rename dtype_map to schema_map for clarity in schema conversion results ([`08268da`](https://github.com/markm-io/dataframe-schema-sync/commit/08268da35dddc8485dd2425ea5eae8c8b0e9d7b9))

## v0.19.0 (2025-03-14)

### Features

- Rename column mapping variable for clarity in schema conversion ([`6022931`](https://github.com/markm-io/dataframe-schema-sync/commit/602293143688ba7b159816805e9f2849235529c7))

## v0.18.0 (2025-03-14)

### Features

- Make schema inference class iterable for tuple unpacking ([`0b0b2a7`](https://github.com/markm-io/dataframe-schema-sync/commit/0b0b2a7065a1a3cee81a6680414b794310a21e35))
- Refactor schema inference to return structured result object with column mappings ([`b68fddf`](https://github.com/markm-io/dataframe-schema-sync/commit/b68fddf42825c547aab2d1d5c355520019637c0c))

## v0.17.0 (2025-03-13)

### Features

- Improve datetime handling in schema inference for timezone awareness ([`a702b80`](https://github.com/markm-io/dataframe-schema-sync/commit/a702b8039205d0c45cef8615f7375bb57de09b9b))

## v0.16.0 (2025-03-13)

### Features

- Enhance datetime conversion logic in schema inference ([`b31c11d`](https://github.com/markm-io/dataframe-schema-sync/commit/b31c11d3a3bc6afcd33f6f3f64dc77a01dbf1eea))

## v0.15.0 (2025-03-13)

### Features

- Add support for standard datetime format in schema inference ([`a031a17`](https://github.com/markm-io/dataframe-schema-sync/commit/a031a17470d5b4785b2b7b7cf8662c0955d43818))

## v0.14.0 (2025-02-24)

### Features

- Add safe conversion methods for string and json handling in schema inference ([`bf88b5b`](https://github.com/markm-io/dataframe-schema-sync/commit/bf88b5b819d9e1c9a45c8f569764b7d44c3edded))

## v0.13.0 (2025-02-14)

### Features

- Add safe conversion methods for string and json handling in schema inference ([`ab6cdc4`](https://github.com/markm-io/dataframe-schema-sync/commit/ab6cdc4610c4fcadca80fda58c5589d96eb60531))

## v0.12.0 (2025-02-14)

### Features

- Enhance dataframe name cleaning with customizable case and truncate limit ([`e8b832b`](https://github.com/markm-io/dataframe-schema-sync/commit/e8b832b041bb509a80b9a647e119e8fc08657a11))

## v0.11.0 (2025-02-13)

### Features

- Add json type handling in schema inference for dataframe conversion ([`f416273`](https://github.com/markm-io/dataframe-schema-sync/commit/f416273aecb7cc16e637322cb5d7d9403856fbb3))

## v0.10.0 (2025-02-13)

### Features

- Add load_config_from_yaml method for dynamic yaml schema loading and improve error handling ([`cf186aa`](https://github.com/markm-io/dataframe-schema-sync/commit/cf186aaf7602ea07049bc2d18c112902559153f4))

## v0.9.0 (2025-02-13)

### Features

- Enhance schema synchronization with dynamic sync_method and improved error handling ([`975773d`](https://github.com/markm-io/dataframe-schema-sync/commit/975773d46fd12539d95ed798ad1a06b338606462))

## v0.8.0 (2025-02-13)

### Features

- Enhance schema synchronization with dynamic sync_method and improved error handling ([`b9c648b`](https://github.com/markm-io/dataframe-schema-sync/commit/b9c648be321cd8ed26069ca03446565f56be1f25))

## v0.7.0 (2025-02-13)

### Features

- Update clean_names method parameters for consistency in dataframe name cleaning ([`8220d39`](https://github.com/markm-io/dataframe-schema-sync/commit/8220d39460b30ace5a6c8b2c639802d95870176c))

## v0.6.0 (2025-02-13)

### Features

- Update sync_schema method to support dynamic sync_method with default value ([`d05444c`](https://github.com/markm-io/dataframe-schema-sync/commit/d05444cb86224aa9d191ededfe7df05c11a705a0))

## v0.5.0 (2025-02-13)

### Features

- Allow dynamic mapping key for yaml schema saving and loading ([`ba74bca`](https://github.com/markm-io/dataframe-schema-sync/commit/ba74bcaf855ab831f1841546f3144fcc12a0da7c))

## v0.4.0 (2025-02-13)

### Features

- Add clean_dataframe_names method using pyjanitor for column name cleaning ([`63620cd`](https://github.com/markm-io/dataframe-schema-sync/commit/63620cdf5f437dc6373399aa4feb0a1f3965449d))

## v0.3.0 (2025-02-13)

### Features

- Enhance yaml schema handling with nested structure and error checks ([`43db66d`](https://github.com/markm-io/dataframe-schema-sync/commit/43db66d45a024b8f033b3e599444c7a16a4e24dd))

### Documentation

- Change html theme to sphinx_material for documentation ([`8eb2b7f`](https://github.com/markm-io/dataframe-schema-sync/commit/8eb2b7f4cf2706e116be4138f78b7f54dcf53fca))
- Change html theme to sphinx_material for documentation ([`cec0334`](https://github.com/markm-io/dataframe-schema-sync/commit/cec0334612bbea022d7da3c1f0b08943460358ab))
- Change html theme to sphinx_material for documentation ([`8946f16`](https://github.com/markm-io/dataframe-schema-sync/commit/8946f16d6150044ae386e7d97f0419409133a193))

## v0.2.0 (2025-02-02)

### Features

- Add schema synchronization with yaml support and enhance type inference ([`cafb11d`](https://github.com/markm-io/dataframe-schema-sync/commit/cafb11dde78ea6c74e6264b9712a574613130e63))

## v0.1.0 (2025-02-02)

### Features

- Add unit tests for schema inference and i/o functionality ([`079e05e`](https://github.com/markm-io/dataframe-schema-sync/commit/079e05e17efea2dfbca805cbd76431233f4cbbf6))
- Add schema i/o functionality with yaml support and update dependencies ([`148a785`](https://github.com/markm-io/dataframe-schema-sync/commit/148a7859876c6fc48d73268ab9cdbfb44cfc1f34))
- Implement schema saving and loading with yaml support ([`2ee9889`](https://github.com/markm-io/dataframe-schema-sync/commit/2ee98893382efd0f8a4004bf4b8e601246f40297))

### Bug fixes

- Remove pycharm files ([`c8dc02c`](https://github.com/markm-io/dataframe-schema-sync/commit/c8dc02c60b82896644040b52d7b5b9939301b1e3))
- Update dependencies in pyproject.toml ([`780d370`](https://github.com/markm-io/dataframe-schema-sync/commit/780d370a2171bd73482916ad970e5bb2b687d69c))

## v0.0.0 (2025-02-01)

### Documentation

- Add @markm-io as a contributor ([`0f06054`](https://github.com/markm-io/dataframe-schema-sync/commit/0f06054556fb7e82fc337e779734ea6edb037c80))
