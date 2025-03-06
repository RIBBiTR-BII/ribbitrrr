
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


#' Extract column metadata from a dbplyr lazy table object
#'
#' @param lazy_tbl A dbplyr lazy table object
#' @param dbcon DBI database connection object
#' @param column_metadata previously fetched column metadata
#' @return A dataframe with schema_name, table_name, and column_name for each output column
#' @export
get_query_metadata <- function(lazy_tbl, dbcon = NULL, column_metadata = NULL) {
  if (is.null(dbcon) & is.null(column_metadata)) {
    warning("Neither database connection (dbcon) nor column_metadata provided. Returning column names only.")
  }

  if (!is.null(dbcon) & is.null(!column_metadata)) {
    warning("Both database connection (dbcon) and column_metadata provided. Using database connection.")
  }

  # Function to extract table name parts from dbplyr_table_path
  extract_table_parts <- function(table_path) {
    # Remove quotes and split by schema/table
    clean_path <- gsub("\"", "", table_path)
    parts <- strsplit(clean_path, "\\.")[[1]]

    if (length(parts) == 2) {
      return(list(schema = parts[1], table = parts[2]))
    } else {
      return(list(schema = NA_character_, table = parts[1]))
    }
  }

  # Function to recursively process a lazy query and extract table information

  process_query <- function(query) {
    tables_info <- list()

    # Process base tables
    if (inherits(query, "lazy_base_query")) {
      if (inherits(query$x, "dbplyr_table_path")) {
        table_parts <- extract_table_parts(query$x)
        tables_info[[length(tables_info) + 1]] <- list(
          schema = table_parts$schema,
          table = table_parts$table,
          columns = query$vars
        )
      }
    }

    # Process joins
    if (!is.null(query$joins) && nrow(query$joins) > 0) {
      for (i in seq_len(nrow(query$joins))) {
        join_table <- query$joins$table[[i]]

        # Process each joined table
        if (is.list(join_table)) {
          # If it's a nested query, process recursively
          if (inherits(join_table, "lazy_query")) {
            nested_tables <- process_query(join_table)
            tables_info <- c(tables_info, nested_tables)
          } else {
            # Process each table in the list
            for (j in seq_along(join_table)) {
              if (inherits(join_table[[j]], "lazy_query")) {
                nested_tables <- process_query(join_table[[j]])
                tables_info <- c(tables_info, nested_tables)
              }
            }
          }
        }
      }
    }

    # Process multi-join queries
    if (inherits(query, "lazy_multi_join_query")) {
      # Extract table names from table_names attribute
      if (!is.null(query$table_names) && "name" %in% names(query$table_names)) {
        for (i in seq_along(query$table_names$name)) {
          table_path <- query$table_names$name[i]
          if (inherits(table_path, "dbplyr_table_path") && !is.na(table_path) && table_path != "") {
            table_parts <- extract_table_parts(table_path)

            # Find columns for this table
            if (!is.null(query$vars) && "table" %in% names(query$vars)) {
              table_cols <- query$vars$name[query$vars$table == i]
              if (length(table_cols) > 0) {
                tables_info[[length(tables_info) + 1]] <- list(
                  schema = table_parts$schema,
                  table = table_parts$table,
                  columns = table_cols
                )
              }
            }
          }
        }
      }

      # Process the x component if it exists
      if (!is.null(query$x) && inherits(query$x, "lazy_query")) {
        nested_tables <- process_query(query$x)
        tables_info <- c(tables_info, nested_tables)
      }
    }

    # Process select queries
    if (inherits(query, "lazy_select_query")) {
      # Process the x component
      if (!is.null(query$x) && inherits(query$x, "lazy_query")) {
        nested_tables <- process_query(query$x)
        tables_info <- c(tables_info, nested_tables)
      }

      # Extract selected columns
      if (!is.null(query$select) && "name" %in% names(query$select)) {
        selected_cols <- query$select$name
        tables_info$selected_columns <- selected_cols
      }
    }

    return(tables_info)
  }

  # Get the lazy query from the tbl object
  lazy_query <- lazy_tbl$lazy_query

  # Process the query to extract table information
  tables_info <- process_query(lazy_query)

  # Find the final selected columns
  final_cols <- NULL

  # Check if we have a vars attribute in the outermost query
  if (!is.null(lazy_query$vars) && "name" %in% names(lazy_query$vars)) {
    final_cols <- lazy_query$vars$name
  } else if (!is.null(names(tables_info))) {
    for (i in seq_along(tables_info)) {
      if (names(tables_info)[i] == "selected_columns") {
        final_cols <- tables_info[[i]]
        break
      }
    }
  } else {
    final_cols = tables_info[[1]]$columns
  }

  # If we still don't have final columns, try to find them in the structure
  if (is.null(final_cols)) {
    # Try to extract from the select attribute if it exists
    if (!is.null(lazy_query$select) && "name" %in% names(lazy_query$select)) {
      final_cols <- lazy_query$select$name
    }
  }

  # Create a mapping of all columns to their tables
  all_columns <- data.frame(
    schema_name = character(),
    table_name = character(),
    column_name = character(),
    stringsAsFactors = FALSE
  )

  for (table_info in tables_info) {
    if (is.list(table_info) && !is.null(table_info$columns) && !is.null(table_info$table)) {
      for (col in table_info$columns) {
        all_columns <- rbind(all_columns, data.frame(
          schema_name = ifelse(is.na(table_info$schema), "survey_data", table_info$schema),
          table_name = table_info$table,
          column_name = col,
          stringsAsFactors = FALSE
        ))
      }
    }
  }

  # Filter to only include columns in the final output
  if (!is.null(final_cols)) {
    result <- all_columns[all_columns$column_name %in% final_cols, ]
  } else {
    result <- all_columns
  }

  # Remove duplicates
  result <- unique(result)

  # identify metadata source
  mdc = NULL

  if (!is.null(dbcon)) {
    mdc = tbl(dbcon, Id("public", "all_columns")) %>%
      collect()
  } else if (!is.null(column_metadata)) {
    mdc = column_metadata
  }

  # finally, join with metadata_columns
  if (!is.null(mdc)) {
    result = result %>%
      left_join(mdc, by = c("schema_name" = "table_schema", "table_name", "column_name"))
  }


  return(result)
}
