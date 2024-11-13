# ribbitrrr

## [0.0.1.4] - 2024-11-13
### Changed

- user prompting for hopToDB() when user and/or password are not provided in .Renviron
- tbl_join() and related aliases default columns = "all"

## [0.0.1.3] - 2024-11-12
### Added

- tbl_left_join()
- tbl_inner_join()
- tbl_full_join()
- tbl_right_join()

### Changed

- tbl_pkey & tbl_fkey now query using mdc binary primary_key and foreign_key columns

## [0.0.1.1] - 2024-11-06
### Added

- key handling for columns which are both primary and foreign

## [0.0.1.0] - 2024-10-30
### Added

- Initial release of the package.
- Included a comprehensive set of functions for data manipulation and analysis.
- Internal check_ambig_table_name() function to avoid ambiguous results when table names are redundant across schemas

### Changed

- Established a more user-friendly interface with clearer function names and arguments.

### Fixed

- Addressed minor bugs in tbl_link function
