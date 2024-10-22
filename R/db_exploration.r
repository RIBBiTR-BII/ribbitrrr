
#' Table primary key
#'
#' Identify the primary key column(s) for a given table in the database metadata. A table's primary key caries a unique and not-null constraint, and is used to uniquely identify the rows in the table. There is only one primary key per table, usually (though not necessarily) single column.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns comprising the primary key for the table provided.
#' @examples 
#' survey_pkey <- tbl_pkey('survey', mdc)
#' @export
tbl_pkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    pull(column_name)
}

#' Table foreign key
#'
#' Identify the foreign key column(s) for a given table in the database metadata. A foreign key points to and mirrors the primary key of another table, establishing relationships between tables. There may be multiple foreign keys in a given table, usually (though not necessarily) single column per key.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with foreign key status in the table provided
#' @examples 
#' survey_fkey <- tbl_fkey('survey', mdc)
#' @export
tbl_fkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == table_str,
           key_type == "FK") %>%
    pull(column_name)
}

#' Table natural key
#'
#' Identify the natural key column(s) for a given table in the database metadata.  A natural key is comprised of the columns which naturally identify a given row, and which are generally used to generate ID columns used as primary keys. There is only one natural key per table, though it may be comprised of multiple columns (a "composite" key).
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with natural key status in the table provided
#' @examples 
#' survey_nkey <- tbl_nkey('survey', mdc)
#' @export
tbl_nkey = function(tbl_name, metadata_columns) {
  metadata_columns %>%
    filter(table_name == table_str,
           natural_key) %>%
    pull(column_name)
}

#' All table keys
#'
#' Identify primary, foreign, and natural key columns for a given table.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The uniqie name(s) of the columns with primary, foreign, or natural key status in the table provided. If a column has multiple statuses, it will show up only once.
#' @examples 
#' survey_keys <- tbl_keys('survey', mdc)
#' db_survey <- tbl(dbcon, "survey") %>%
#'                 select(all_of(survey_keys)) %>%
#'                 collect()
#' @export
tbl_keys = function(tbl_name, metadata_columns) {
  return(unique(c(
    tbl_pkey(table_str, metadata_columns=metadata_columns),
    tbl_nkey(table_str, metadata_columns=metadata_columns),
    tbl_fkey(table_str, metadata_columns=metadata_columns)
  )))
}

# in dev

tbl_link = function(tbl_name, metadata_columns) {
  link = list()
  link[["root"]] = tbl_name
  
  parents = list()
  
  tbl_current = tbl_name
  
  fkey_vec = tbl_fkey(tbl_name, mdc)
  
  for (ff in 1:length(fkey_vec)) {
    fkey = fkey_vec[ff]
    
    tbl_parent = metadata_columns %>%
      filter(column_name == fkey,
             key_type == "PK") %>%
      pull(table_name)
    
    nkey = tbl_nkey(tbl_parent, metadata_columns)
    
    parents[[ff]] = list(name=tbl_parent,
                         pkey=fkey,
                         nkey=nkey)
  }
  link[["parents"]] = parents
  return(link)
}


tbl_join = function(dbcon, chain) {
  
  tbl = tbl(dbcon, chain$root)
  
  for (ii in 1:length(chain$parents)) {
    tbl_next = tbl(dbcon, chain$parents[[ii]]$name)
    
    tbl = tbl %>%
      left_join(tbl_next, by = c(chain$parents[[ii]]$pkey))
  }
  
  return(tbl)
}
