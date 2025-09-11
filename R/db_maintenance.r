#' Compare data for staging
#'
#' Compare new data with a corresponding existing database table prior to writing, to identify rows in the new data with keys which are not yet found in the existing data ("insert"), rows which are identical in all shared columns to rows in the existing data ("duplicate"), rows with existing key combination but different non-key values in the existing data ("update"), and rows in the existing data with key combinations which do not exist in the new data ("orphan")
#' @param data_old A reference data frame containing the existing data
#' @param data_new A data frame containing the new data to be compared with the existing data
#' @param key_columns A character vector of key columns used to distinguish "insert", "update", and "orphan" rows. Can be a primary key or natural key, depending on needs.
#' @param return_all Logical: do you want to perform all comparisons (insert, update, orphan, duplicate)? Overrides previous logical parmeters. False by default.
#' @param insert Logical: do you want to check for new rows to insert? TRUE by default.
#' @param update Logical: do you want to check for new rows to update? TRUE by default.
#' @param orphan Logical: do you want to check for orphan rows in the old data? FALSE by default.
#' @param duplicate Logical: do you want to check for duplicate rows? FALSE by default.
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
compare_for_staging = function(data_old, data_new, key_columns, return_all=TRUE, insert=TRUE, update=TRUE, orphan=FALSE, duplicate=FALSE, report=FALSE){

  # Check for key column duplicates
  old_dups = nrow(data_old %>%
                    select(all_of(key_columns)) %>%
                    group_by_at(key_columns) %>%
                    summarise(k_count = n()) %>%
                    filter(k_count > 1))

  if (old_dups != 0) {
    stop("Duplicate key_column combinations found in data_old. Comparison aborted.")
  }

  # Check for key column duplicates
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
  # logic: find duplicate rows on common columns (ignore unmatched columns)
  data_bind = bind_rows(data_new[common_columns], data_old[common_columns])
  data_duplicate = data_bind[duplicated(data_bind),]
  if (duplicate || return_all){
    output[["duplicate"]] = data_duplicate
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

    update_cols = setdiff(common_columns, key_columns)

    data_update_new = data_update_keys %>%
      inner_join(data_new, by=key_columns) %>%
      inner_join(data_old %>%
                   select(-all_of(update_cols)), by=key_columns) %>%
      select(all_of(key_columns),
             all_of(colnames(data_old)))


    data_update_old = data_update_keys %>%
      inner_join(data_old, by=key_columns) %>%
      select(all_of(key_columns),
             all_of(colnames(data_old)))

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


#' Distinguishing columns to update
#'
#' Using output results from \link[ribbitrrr]{compare_for_staging}, identifies which columns differ for a given set of rows and outputs a comparison table for each column.
#' @param cfs_results Results from \link[ribbitrrr]{compare_for_staging} which contain updates (either update = TRUE or return_all = TRUE)
#' @param id_cols name(s) of ID column(s) to be reported alongside difference data
#' @return A list of dataframes corresponding to columns which differe between update and update_old data.
#' @examples
#'
#' data_old = data.frame(id= 1:5, values = c("a", "b", "c", "d", "e"))
#' data_new = data.frame(id = 3:7, values = c("d", "d", "f", "f", "g"))
#'
#' comparison <- compare_for_staging(data_old, data_new, key_columns = c("id"), return_all = TRUE)
#' update_diffs <- compare_updates(comparison)
#' @importFrom dplyr %>% rename
#' @export
#'
compare_updates = function(cfs_results, id_cols = NULL) {

  df1 = cfs_results$update
  df2 = cfs_results$update_old

  if (nrow(df1) == 0) {
    warning("No updates found.")
    return(NULL)
  } else {

    diffs_by_col = list()
    # Compare the contents of each column
    for (col in names(df1)) {
      # Use is.na() to handle NA values and compare non-NA values
      if (!identical(is.na(df1[[col]]), is.na(df2[[col]])) ||
          !all(df1[[col]] == df2[[col]], na.rm = TRUE)) {

        # Create a logical vector for differences, considering NA values
        diff_mask <- is.na(df1[[col]]) != is.na(df2[[col]]) |
          ((!is.na(df1[[col]]) & !is.na(df2[[col]])) & (df1[[col]] != df2[[col]]))


        diff_df <- data.frame(
          new_data = df1[[col]][diff_mask],
          old_data = df2[[col]][diff_mask],
          row = which(diff_mask)
        )

        if (!is.null(id_cols)) {
          diff_df = bind_cols(df1 %>%
                                select(any_of(id_cols)) %>%
                                filter(diff_mask),
                              diff_df)
        }


        diff_df = diff_df %>%
          rename(setNames("new_data", paste0(col, "_new")),
                 setNames("old_data", paste0(col, "_old")))

        diffs_by_col[[col]] = diff_df
      }
    }

    difcols = names(diffs_by_col)
    if (length(difcols > 0)) {
      message("Differences found in columns:")
      for (ii in difcols) {
        message(paste0("\t", ii))
      }
    }


    return(diffs_by_col)
  }
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
#' @importFrom DBI dbExecute dbWriteTable dbBegin dbCommit dbRollback
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


#' This set of functions support the modification of tables in a remote database .
#'
#' This set of functions allow you to modify a remote database table by first pushing new data to a temporary table with \link[ribbitrrr]{stage_to_temp}, then maiking use of the corresponding \link[dplyr]{rows} functions.
#'  * db_rows_insert(dbcon, reference_table, novel_data, pkey)
#'  * db_rows_update(dbcon, reference_table, novel_data, pkey)
#'  * db_rows_upsert(dbcon, reference_table, novel_data, pkey)
#'  * db_rows_delete(dbcon, reference_table, novel_data, pkey)
#'
#'
#' @param dbcon Database connection (A valid and active DBI database connection object)
#' @param reference_table A table pointer object pointing to the database table which you want to modify.
#' @param novel_data Data Frame of rows to be pushed to the database
#' @param pkey column name(s) for primary key used to join novel data with reference table
#' @return An updated table pointer object (for future use).
#' @examples
#' if(FALSE) {
#'   dbcon = HopToDB("ribbitr")
#'   db_capture = dplyr::tbl(dbcon, "capture")
#'   db_capture = db_rows_upsert(dbcon, db_capture, novel_capture, "capture_id")
#' }
#' @importFrom dplyr tbl rows_insert
#' @export
db_rows_insert <- function(dbcon, reference_table, modified_data, pkey) {
  temp_table = stage_to_temp(dbcon, reference_table, modified_data)
  temp_pointer = tbl(dbcon, temp_table)
  db_mod = rows_insert(reference_table, temp_pointer, by=pkey, in_place=TRUE, conflict = "ignore")
  return(db_mod)
}


#' @rdname db_rows_insert
#' @importFrom dplyr tbl rows_update
#' @export
db_rows_update <- function(dbcon, reference_table, modified_data, pkey) {
  temp_table = stage_to_temp(dbcon, reference_table, modified_data)
  temp_pointer = tbl(dbcon, temp_table)
  db_mod = rows_update(reference_table, temp_pointer, by=pkey, in_place=TRUE, unmatched = "ignore")
  return(db_mod)
}


#' @rdname db_rows_insert
#' @importFrom dplyr tbl rows_upsert
#' @export
db_rows_upsert <- function(dbcon, reference_table, modified_data, pkey) {
  temp_table = stage_to_temp(dbcon, reference_table, modified_data)
  temp_pointer = tbl(dbcon, temp_table)
  db_mod = rows_upsert(reference_table, temp_pointer, by=pkey, in_place=TRUE)
  return(db_mod)
}


#' @rdname db_rows_insert
#' @importFrom dplyr tbl rows_delete
#' @export
db_rows_delete <- function(dbcon, reference_table, modified_data, pkey) {
  temp_table = stage_to_temp(dbcon, reference_table, modified_data)
  temp_pointer = tbl(dbcon, temp_table)
  db_mod = rows_delete(reference_table, temp_pointer, by=pkey, in_place=TRUE, unmatched = "ignore")
  return(db_mod)
}


#' Identify and resolve sample name conflicts
#'
#' Identifies sample_name/sample_type conflicts 1) within new data, 2) between new and old data, and 3) between new and old, previously conflicting and resolved data.
#' @param data Cleaned new sample data to be compared for conflicts
#' @param db_sample A lazy table object pointing to the sample database table ("old data")
#' @param novel_data Data Frame of rows to be pushed to the database
#' @return Returns a table of new data plus any old data to be updated. This table should be upserted into database.
#' @importFrom dplyr %>% mutate collect bind_rows select distinct inner_join group_by ungroup filter
#' @export
resolve_sample_conflicts = function(data, db_sample) {
  # identify 3 types of conflicts
  # 1) sample name conflicts in new data
  # 2) sample name conflicts between new and existing data
  # 3) sample name conflicts between new and existing (and already revised) data

  data_new = data %>%
    mutate(dupe = FALSE,
           data = "new",
           drop = FALSE)

  data_sample = db_sample %>%
    collect() %>%
    mutate(data = "old")

  new_n = nrow(data_new %>%
                 janitor::get_dupes(sample_name, sample_type) %>%
                 select(sample_name,
                        sample_type) %>%
                 distinct())

  data_old_first = bind_rows(data_sample,
                             data_new)
  conflict_old_first = janitor::get_dupes(data_old_first, sample_name, sample_type) %>%
    select(sample_name, sample_type) %>%
    distinct()

  old_first = data_sample %>%
    inner_join(conflict_old_first, by = c("sample_name", "sample_type")) %>%
    mutate(drop = FALSE)

  old_first_n = nrow(old_first)

  old_revised = data_sample %>%
    filter(sample_name_conflict %in% data_new$sample_name) %>%
    mutate(drop = TRUE,
           sample_name = sample_name_conflict)

  old_revised_n = nrow(old_revised %>%
                         select(sample_name,
                                sample_type) %>%
                         distinct())

  data_r = bind_rows(old_first,
                     old_revised,
                     data_new)

  data_s = data_r %>%
    group_by(sample_name, sample_type) %>%
    mutate(row_num = row_number(),
           dupe = ifelse(n() > 1, TRUE, FALSE),
           sample_name_conflict = ifelse(dupe,
                                         sample_name,
                                         NA_character_)) %>%
    ungroup() %>%
    mutate(sample_name = ifelse(dupe,
                                paste0(sample_name, "_", letters[row_num]),
                                sample_name),
           uuid_name = paste0(sample_name, sample_type),
           sample_id = ifelse(dupe & data == "new", UUIDfromName("1208e62f-d3a1-462c-984f-0bf1f43f5837", uuid_name), sample_id)) %>%
    filter(!drop) %>%
    select(-row_num,
           -drop)

  if (new_n == 0 & old_first_n == 0 & old_revised_n == 0) {
    cat("No sample name/type conflicts found.")
  } else {
    cat("Sample name/type conflicts found:\n\tnew conflicts: ", new_n, "\n\tnew/old conflicts: ", old_first_n, "\n\tnew/old(previously-revised) conflicts: ", old_revised_n, sep = "")
  }

 return(data_s)
}

#' Check for valid values in catagorical variables
#'
#' Compares caagorical varibles with the defined catagories in column metadata, to make sure they are validprior to pushing.
#' @param data Data table with column names matching a given table in the column metadata
#' @param table corresponding table name in column metadata (str)
#' @param novel_data column metadata pulled from database
#' @return Silently returns same data table with no revisions. Throws warning with report on invalid data if found.
#' @importFrom dplyr %>% pull
#' @export
catagorical_check = function(data, table, mdc){
  mdc_data = mdc %>%
    filter(table_name == table)

  cat_cols = mdc_data$column_name[grepl("^\\[.*\\]$", mdc_data$format)]

  cat_cols_present = intersect(cat_cols, colnames(data))

  invalid = FALSE

  w_message = ""

  for (cc in cat_cols_present) {
    mdc_row = mdc_data[mdc_data$column_name == cc,]

    data_na = any(is.na(data[cc]))
    mdc_na = mdc_row$is_nullable

    data_vals = unique(data[cc]) %>%
      pull(cc)
    mdc_vals = strsplit(gsub("\\[|\\]", "", mdc_row$format), ",\\s*")[[1]]

    if (mdc_na) {
      mdc_vals = c(mdc_vals, NA)
    }

    invalid_vals = setdiff(data_vals, mdc_vals)

    if (length(undefined_vals) > 0) {
      undefined = TRUE
      w_message = paste0(w_message, "\n\tColumn: ", cc, "\n\t\tDefined values: ", paste(mdc_vals, collapse = ", "), "\n\t\tUndefined values: ", paste(undefined_vals, collapse = ", "), "\n")
      }
  }

  if (undefined) {
    warning(paste0("Invalid values found in catagotical data:\n", w_message))
  }

  return(data)
}
