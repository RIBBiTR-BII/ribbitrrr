# ribbitrrr
## [0.0.2.1] - 2025-04-07
### Added

- get_ribbitr_metadata() function for subsetting column metadata from the metadata_columns table to match columns of a dbplyr query

## [0.0.2.0] - 2025-02-14
### Added

- hopRegister() function for viewing and browsing connections made with hopToDB() in the RStudio Connections Pane

### Changed

- improved error handling for hopToDB() connection parameters

## [0.0.1.6] - 2024-12-18
### Added

- scrape_amphibiaweb() function

## [0.0.1.5] - 2024-11-13
### Changed

- patch tbl_link to handle fkey reference columns which differ in name from fkey column
- patch tbl_chain to handle loops in table graph

## [0.0.1.4] - 2024-11-13
### Changed

- user prompting for hopToDB() when user and/or password are not provided in .Renviron
- tbl_join() and related aliases default columns = "all"
- prohibit internal duplicates in compare_for_staging

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
