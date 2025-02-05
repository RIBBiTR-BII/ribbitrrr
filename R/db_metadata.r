
#' @importFrom DBI dbGetQuery
#' @keywords internal
pull_schemas <- function(dbcon) {
  schemas <- dbGetQuery(dbcon, "SELECT schema_name FROM information_schema.schemata
                      WHERE schema_name NOT LIKE 'pg_temp_%'
                      AND schema_name NOT LIKE 'pg_toast_temp_%'
                      AND schema_name != 'pg_catalog'
                      AND schema_name != 'information_schema'
                      AND schema_name != 'public';")$schema_name
}

#' @importFrom dplyr tbl
#' @keywords internal
scrape_pg_table_data <- function(dbcon, schema) {
  query <- paste0("
    SELECT
      t.table_schema, t.table_name,
      (SELECT count(*) FROM information_schema.columns c WHERE c.table_name = t.table_name AND c.table_schema = t.table_schema) as column_count,
      pg_catalog.obj_description(format('%s.%s',t.table_schema,t.table_name)::regclass::oid, 'pg_class') as table_description
    FROM
      information_schema.tables t
    WHERE
      t.table_schema = '", schema, "'
  ")

  tables <- tbl(dbcon, sql(query))

  return(tables)
}

#' @importFrom dplyr tbl
#' @keywords internal
scrape_pg_column_data <- function(dbcon, schema) {
  colunm_query <- paste0("
    SELECT
      c.table_schema,
      c.table_name,
      c.column_name,
      c.data_type,
      c.character_maximum_length,
      c.numeric_precision,
      c.datetime_precision,
      c.is_nullable,
      c.column_default,
      c.ordinal_position,
      pg_catalog.col_description(format('%s.%s',c.table_schema,c.table_name)::regclass::oid, c.ordinal_position) as pg_description
    FROM
      information_schema.columns c
    WHERE
      c.table_schema = '", schema, "'
    ORDER BY
      c.table_name, c.ordinal_position
  ")

  columns <- tbl(dbcon, sql(column_query)) %>% collect()

  constraint_query <- paste0("
    SELECT tc.table_schema, tc.table_name, kcu.column_name, tc.constraint_name, tc.constraint_type
    FROM
      information_schema.key_column_usage kcu
    LEFT JOIN
      information_schema.table_constraints tc
      ON kcu.table_schema = tc.table_schema
      AND kcu.table_name = tc.table_name
      AND kcu.constraint_name = tc.constraint_name
    WHERE
      kcu.table_schema = '", schema, "'
  ")

  constraints <-tbl(dbcon, sql(constraint_query)) %>% collect()

  pkeys = constraints %>%
    filter(constraint_type == "PRIMARY KEY") %>%
    select(table_name,
           column_name) %>%
    mutate(primary_key = TRUE)

  fkeys = constraints %>%
    filter(constraint_type == "FOREIGN KEY") %>%
    select(table_name,
           column_name) %>%
    mutate(foreign_key = TRUE)

  unique = constraints %>%
    filter(constraint_type == "UNIQUE") %>%
    select(table_name,
           column_name) %>%
    mutate(unique = TRUE)

  fkey_refs_query <- paste0("
    SELECT
        kcu.table_schema AS table_schema,
        kcu.table_name AS table_name,
        kcu.column_name AS column_name,
        ccu.table_schema AS fkey_ref_schema,
        ccu.table_name AS fkey_ref_table,
        ccu.column_name AS fkey_ref_column
    FROM
        information_schema.table_constraints AS tc
    JOIN
        information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.constraint_schema = kcu.constraint_schema
    JOIN
        information_schema.referential_constraints AS rc
        ON tc.constraint_name = rc.constraint_name
        AND tc.constraint_schema = rc.constraint_schema
    JOIN
        information_schema.key_column_usage AS ccu
        ON rc.unique_constraint_name = ccu.constraint_name
        AND rc.unique_constraint_schema = ccu.constraint_schema
    WHERE
        tc.constraint_type = 'FOREIGN KEY'
    ORDER BY
        kcu.table_name, kcu.column_name
  ")

  fkey_refs <-tbl(dbcon, sql(fkey_refs_query)) %>% collect()

  columns_constraints = columns %>%
    left_join(pkeys, by = c("table_name", "column_name")) %>%
    left_join(fkeys, by = c("table_name", "column_name")) %>%
    left_join(unique, by = c("table_name", "column_name")) %>%
    left_join(fkey_refs, by = c("table_schema", "table_name", "column_name")) %>%
    mutate(primary_key = ifelse(is.na(primary_key), FALSE, TRUE),
           foreign_key = ifelse(is.na(foreign_key), FALSE, TRUE),
           unique = ifelse(is.na(unique), FALSE, TRUE),
           key_type = case_when(  # depreciate on Jan 2025
             primary_key ~ "PK",
             foreign_key ~ "FK",
             TRUE ~ NA_character_)) %>%
    select(table_schema,
           table_name,
           column_name,
           primary_key,
           foreign_key,
           unique,
           is_nullable,
           everything())


  return(columns_constraints)
}

#' @keywords internal
tables_pkey = c("table_schema", "table_name")

#' @keywords internal
columns_pkey = c("table_schema", "table_name", "column_name")

report_metadata_changes <- function(dbcon, schema, table_metadata=TRUE, column_metadata=TRUE, insert=TRUE, update=TRUE, orphan=TRUE) {

}
