#' Compare data for staging
#'
#' Compare new data with a corresponding existing database table prior to writing, to identify rows in the new data with keys which are not yet found in the existing data ("insert"), rows which are identical in all shared columns to rows in the existing data ("duplicate"), rows with existing key combination but different non-key values in the existing data ("update"), and rows in the existing data with key combinations which do not exist in the new data ("orphan")
#' @param data_old A reference data frame containing the existing data
#' @param data_new A data frame containing the new data to be compared with the existing data
#' @param key_columns A character vector of key columns used to distinguish "insert", "update", and "orphan" rows. Can be a primary key or natural key, depending on needs.
#' @param insert Logical: do you want to check for new rows to insert? TRUE by default.
#' @param update Logical: do you want to check for new rows to update? TRUE by default.
#' @param orphan Logical: do you want to check for orphan rows in the old data? FALSE by default.
#' @param duplicate Logical: do you want to check for duplicate rows? FALSE by default.
#' @param return_all Logical: do you want to perform all comparisons (insert, update, orphan, duplicate)? Overrides previous logical parmeters. False by default.
#' @param report String name for report if desired. If provided, function will announce the lengths of all provided outputs.
#' @return A list of dataframes containing rows corresponding to each comparison (insert, update, orphan, duplicate)
#' @examples
#'
#' data_old = data.frame(id= 1:5, values = c("a", "b", "c", "d", "e"))
#' data_new = data.frame(id = 3:7, values = c("d", "d", "f", "f", "g"))
#'
#' comparison <- compare_for_staging(data_old, data_new, key_columns = c("id"), return_all = TRUE)
#' data_to_insert = comparison$insert
#' data_to_update = comparison$update
#' data_orphans = comparison$orphan
#' data_duplicates = comparison$duplicate
#' @importFrom dplyr %>% bind_rows anti_join group_by_at count ungroup mutate filter select inner_join
#' @export
#'
compare_for_staging = function(data_old, data_new, key_columns, insert=TRUE, update=TRUE, orphan=FALSE, duplicate=FALSE, return_all=FALSE, report=FALSE){

  # Checks
  old_dups = nrow(data_old %>%
                    select(all_of(key_columns)) %>%
                    group_by_at(key_columns) %>%
                    summarise(k_count = n()) %>%
                    filter(k_count > 1))

  if (old_dups != 0) {
    stop("Duplicate key_column combinations found in data_old. Comparison aborted.")
  }

  new_dups = nrow(data_new %>%
                    select(all_of(key_columns)) %>%
                    group_by_at(key_columns) %>%
                    summarise(k_count = n()) %>%
                    filter(k_count > 1))

  if (new_dups != 0) {
    stop("Duplicate key_column combinations found in data_new. Comparison aborted.")
  }

  output = list()

  common_columns = intersect(colnames(data_old), colnames(data_new))

  # DUPLICATE: rows identical in both data_old and data_new
  # logic: find duplicate rows
  data_bind = bind_rows(data_new, data_old)
  data_duplicate = data_bind[duplicated(data_bind),]
  if (duplicate || return_all){
    duplicate_new = data_duplicate %>%
      inner_join(data_new, by=common_columns)
    duplicate_old = data_duplicate %>%
      inner_join(data_old, by=common_columns)

    output[["duplicate"]] = duplicate_new
    output[["duplicate_old"]] = duplicate_old
  }

  # bind rows ignoring duplicates, tracking source
  data_bind_uni = data_bind %>%
    anti_join(data_duplicate, by = key_columns)

  # count grouped by key_columns (1 or 2)
  data_pkey_counts = data_bind_uni %>%
    group_by_at(key_columns) %>%
    count() %>%
    ungroup()

  data_bind_source = bind_rows(data_new %>%
                                 mutate(d_source = "a"),
                               data_old %>%
                                 mutate(d_source = "b"))

  # ORPHAN: rows from data_old with unique key_columns
  # logic: pkey compo found 1x in data_bind, from data_old
  if (orphan || return_all){
    data_orphan = data_pkey_counts %>%
      filter(n == 1) %>%
      select(-n) %>%
      inner_join(data_old, by=key_columns)

    output[["orphan"]] = data_orphan
  }

  # INSERT: rows from data_new with unique keys columns
  # logic: pkey combo found 1x in data_bind, from data_new
  if (insert || return_all){
    data_insert = data_pkey_counts %>%
      filter(n == 1) %>%
      select(-n) %>%
      inner_join(data_new, by=key_columns)

    output[["insert"]] = data_insert
  }


  # UPDATE: rows from data_new with key-column combination found in data_old, but with distinctions among non-key columns
  # logic: pkey combo found 2x in data_bind, is distinct among non-pkey columns
  if (update || return_all){
    data_update_keys = data_pkey_counts %>%
      filter(n == 2) %>%
      select(-n)

    data_update_new = data_update_keys %>%
      inner_join(data_new, by=key_columns)

    data_update_old = data_update_keys %>%
      inner_join(data_old, by=key_columns)

    output[["update"]] = data_update_new
    output[["update_old"]] = data_update_old
  }

  # report if called
  if (!isFALSE(report)) {
    cat(report)
    for (nn in names(output)) {
      cat("\n\t", nn, "\t", nrow(output[[nn]]))
    }
    cat("\n")
  }

  return(output)
}

#' Stage data to temporary database table
#'
#' This function allows you to push new data to a temporary table of the same constraints/datatypes as the table you want to eventually push to, to make sure everything checks out. Once data is staged, it can be pushed (inserted, updated, upserted) to the corresponding database table using ____.
#' @param dbcon Database connection (A valid and active DBI database connection object)
#' @param reference_table A lazy table object pointing to the database table which you want to push data to.
#' @param novel_data Data Frame of rows to be pushed to the database
#' @return If successful transaction, the name of the temporary table holding the data.
#' @examples
#' if(FALSE) {
#'   dbcon = HopToDB("ribbitr")
#'   db_capture = dplyr::tbl(dbcon, "capture")
#'   temp_capture_tbl = stage_to_temp(dbcon, db_capture, novel_capture)
#'   pointer = dplyr::tbl(dbcon, temp_capture_tbl)
#'   dbplyr::rows_upsert(db_capture, pointer, by=capture_pkey, in_place=TRUE)
#' }
#' @importFrom DBI dbExecute dbWriteTable
#' @export
stage_to_temp <- function(dbcon, reference_table, novel_data) {
  # check that all novel_data columns exist in reference table
  ref_cols = colnames(reference_table)
  nov_cols = colnames(novel_data)


  # build meaningful temp table name: [schema].[reference_table]_temp
  table_path = as.character(reference_table$lazy_query$x)
  path_parts = strsplit(gsub("\"", "", table_path), "\\.")
  schema_name = path_parts[[1]][1]
  table_name = path_parts[[1]][2]
  temp_table_name = paste0(schema_name, "_", table_name, "_temp")

  novel_columns = setdiff(nov_cols, ref_cols)

  # stop if novel columns found
  if (length(novel_columns) > 0) {
    stop(paste0("The following columns were in novel data which are absent from reference_table ", table_name, ": ", paste(novel_columns, collapse=", ")))
  }

  # begin transaction

  # drop temp table if exists
  suppressMessages(
    dbExecute(dbcon, paste0("DROP TABLE IF EXISTS ", temp_table_name, ";"))
  )
  # copy reference table to temporary table
  dbExecute(dbcon, paste0("CREATE TEMP TABLE ", temp_table_name, " AS SELECT * FROM ", schema_name, ".", table_name, ";"))
  # drop all existing rows
  dbExecute(dbcon, paste0("TRUNCATE TABLE ", temp_table_name))
  # drop all columns in reference_table not in novel_data
  drop_cols = setdiff(ref_cols, nov_cols)
  if (length(drop_cols) > 0){
    dbExecute(dbcon, paste0("ALTER TABLE ", temp_table_name, " DROP COLUMN ", paste(drop_cols, collapse = ", DROP COLUMN ")))
  }
  # write all novel data to temp table
  dbWriteTable(dbcon, name = temp_table_name, value = novel_data, append = TRUE)


  return(temp_table_name)
}
