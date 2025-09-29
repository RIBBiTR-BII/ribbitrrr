conn_session_context <- new.env(parent = emptyenv())

#' @keywords internal
conn_session_set <- function(id, vals = list()) {
  conn_session_context[[id]] <- vals
}

#' @keywords internal
conn_session_get <- function(id) {
  if (id == "") {
    return(NULL)
  }
  conn <- conn_session_context[[id]]
  if (is.null(conn)) {
    stop("No metadata was found for this connection")
  } else {
    conn
  }
}

#' @keywords internal
#' @importFrom dplyr %>% mutate arrange select
#' @importFrom rscontract rscontract_spec rscontract_close rscontract_open
hopRegister = function(dbcon, dbhost, dbname, connection_id, type) {

  up_query = "SELECT r.rolname AS user_name,
  c.oid::regclass AS table_name_full,
  p.perm AS privilege_type
  FROM pg_class c
  CROSS JOIN pg_roles r
  CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) p(perm)
  WHERE relkind IN ('r', 'v')
  AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname in ('pg_catalog','information_schema'))
  AND has_table_privilege(rolname, c.oid, p.perm)
  and r.rolname = current_user;"

  user_priv = dbGetQuery(dbcon, up_query) %>%
    mutate(schema_name = ifelse(grepl("\\.", table_name_full), sub("\\..*", "", table_name_full), "public"),
           table_name = sub(".*\\.", "", table_name_full)) %>%
    # separate(col = table_name, into = c("schema_name", "table_name"), sep = "\\.", fill = "left") %>%
    arrange(schema_name, table_name) %>%
    select(schema_name, table_name)

  session <- conn_session_get(connection_id)
  if (is.null(session)) {
    name <- as.character(dbname)
    host <- as.character(dbhost)
    type <- as.character(type)
  } else {
    name <- session$name
    host <- session$host
    type <- session$type
  }

  spec_contract <- rscontract_spec(
    type = type,
    name = name,
    host = host,
    connect_script = paste0("ribbitrrr::hopToDB('", dbname, "')"),
    disconnect_code = function() {
      rscontract_close(host = host, type = type)
      dbDisconnect(dbcon)
    },
    object_types = function() {
      list(
        schema = list(
          contains = list(table = list(contains = "data"),
                          view = list(contains = "data")))
      )
    },
    object_list = function(schema = NULL) {

      if (is.null(schema)) {
        schemas = unique(user_priv$schema_name)

        return(
          data.frame(
            name = schemas,
            type = rep("schema", times = length(schemas)),
            stringsAsFactors = FALSE
          ))

      } else if (is.character(schema)) {
        tables = unique(user_priv %>%
                          filter(schema_name == schema) %>%
                          pull(table_name))

        return(
          data.frame(
            name = tables,
            type = rep("table", times = length(tables)),
            stringsAsFactors = FALSE
          ))
      }
    },
    object_columns = function(table, schema) {
      rs <- dbSendQuery(dbcon, paste0("SELECT * FROM ", schema, ".", table, " LIMIT 0"))
      cols = dbColumnInfo(rs)
      dbClearResult(rs)

      return(
        data.frame(
          name = cols[["name"]],
          type = cols[["type"]],
          stringsAsFactors = FALSE
        ))

    },
    preview_code = function(rowLimit, schema, table) {
      # attempt to query table
      tryCatch({
        dbGetQuery(dbcon, paste0("SELECT * FROM ", schema, ".", table, " LIMIT ", rowLimit))
      },
      error=function(e) {
        print(e$message)
        if (grepl("permission denied for schema", e$message)) {
          message(cat("ERROR: permission denied for schema ", schema, "\n", sep = ""))
        } else {
          message(e$message)
        }
      })
    }
  )
  rscontract_open(spec_contract)

}

#' hopToDB
#'
#' A simple function to easily to the RIBBiTR (or another) remote database. The following database connection credentials must be saved to your local .Renviron file(see \link[DBI]{dbConnect}): dbname - the database name, host - the database host, port - the database port, user - your database username, password - your database password. Variables may be defined with an optional prefix seperated by and underscore (e.g. "ribbitr_dbname") to distinguish multiple connections.
#' @param prefix an optional prefix (string) added to the front of connection credential variables to distinguish between sets of credentials.
#' @param timezone an optional timezone parameter to help convert data from various timezones to your local time.
#' @param yaml path to YAML file containing login credentials. Default of NA assumes credentials are stored in .Renviron file
#' @param hopReg Do you want to be able to view and browse this connection in the RStudio Connections Pane?
#' @return database connection object, to be passed to other functions (e.g. \link[DBI]{dbListTables}, \link[dplyr]{tbl}, etc.).
#' @examples
#'
#' ## open your local .Renviron file
#' # usethis::edit_r_environ
#'
#' ## copy the following to .Renviron, replacing corresponding database credentials
#'
#' # ribbitr.dbname = "[DATABASE_NAME]"
#' # ribbitr.host = "[DATABASE_HOST]"
#' # ribbitr.port = "[DATABASE_PORT]"
#' # ribbitr.user = "[USERNAME]"
#' # ribbitr.password = "[PASSWORD]"
#'
#' ## connect to your database with a single line of code
#' dbcon <- hopToDB("ribbitr")
#'
#' ## or for greater security
#' ## copy the following to .Renviron, replacing corresponding database credentials and omitting user and/or password
#'
#' # ribbitr.dbname = "[DATABASE_NAME]"
#' # ribbitr.host = "[DATABASE_HOST]"
#' # ribbitr.port = "[DATABASE_PORT]"
#'
#' ## connect to your database with a single line of code
#' dbcon <- hopToDB("ribbitr")
#'
#' # user is prompted for user an/or password
#'
#' @importFrom DBI dbConnect dbDriver dbListTables
#' @importFrom RPostgres Postgres
#' @importFrom stats na.omit
#' @importFrom yaml yaml.load_file
#' @importFrom rstudioapi executeCommand
#' @export
hopToDB = function(prefix = NA, timezone = NULL, yaml = NA, hopReg = TRUE) {
  drv = dbDriver("Postgres")
  dbname_var = paste(na.omit(c(prefix, "dbname")), collapse = ".")
  host_var = paste(na.omit(c(prefix, "host")), collapse = ".")
  port_var = paste(na.omit(c(prefix, "port")), collapse = ".")
  user_var = paste(na.omit(c(prefix, "user")), collapse = ".")
  password_var = paste(na.omit(c(prefix, "password")), collapse = ".")

  if (is.na(yaml)) {
    # default to .Renviron, if no yaml provided

    # attempt to fetch dbname
    dbname = Sys.getenv(dbname_var)
    if (dbname == "") {
      stop(paste0("Specified dbname not found in .Renviron: '", dbname_var, "'. Have these database parameters been set up?"), call. = FALSE)
    }

    # attempt to fetch host
    host = Sys.getenv(host_var)
    if (host == "") {
      stop(paste0("Specified host not found in .Renviron: '", host_var, "'"), call. = FALSE)
    }

    # attempt to fetch host
    port = Sys.getenv(port_var)
    if (port == "") {
      stop(paste0("Specified port not found in .Renviron: '", port_var, "'"), call. = FALSE)
    }

    # attempt to fetch user
    tryCatch({
      user = Sys.getenv(user_var)
    },
    error=function(e) {
      if (!grepl("object 'user' not found", e$message)) {
        message(e$message)
      }
    })

    # attempt to fetch password
    tryCatch({
      password = Sys.getenv(password_var)
    },
    error=function(e) {
      if (!grepl("object 'password' not found", e$message)) {
        message(e$message)
      }
    })
  } else {
    # load from yaml
    creds = yaml.load_file(yaml)

    # attempt to fetch dbname
    dbname = creds[[dbname_var]]
    if (is.null(dbname)) {
      stop(paste0("Specified dbname ('", dbname_var, "') not found in YAML. Have these database parameters been set up?"), call. = FALSE)
    }

    # attempt to fetch host
    host = creds[[host_var]]
    if (is.null(host)) {
      stop(paste0("Specified host ('", host_var, "') not found in YAML."), call. = FALSE)
    }

    # attempt to fetch host
    port = creds[[port_var]]
    if (is.null(port)) {
      stop(paste0("Specified port ('", port_var, "') not found in YAML."), call. = FALSE)
    }

    # attempt to fetch user
    tryCatch({
      user = creds[[user_var]]
    },
    error=function(e) {
      if (!grepl("object 'user' not found", e$message)) {
        message(e$message)
      }
    })

    # attempt to fetch password
    tryCatch({
      password = creds[[password_var]]
    },
    error=function(e) {
      if (!grepl("object 'password' not found", e$message)) {
        message(e$message)
      }
    })

  }



  # use prefix for database_name if provided, otherwise dbname
  if (!is.na(prefix)) {
    database_name = prefix
  } else {
    database_name = dbname
  }


  tryCatch({
    cat("Connecting to '", database_name, "'... ", sep = "")

    if (user == "") {
      user = rstudioapi::askForPassword(paste0("Username for '", database_name, "':"))
    }

    if (password == "") {
      password = rstudioapi::askForPassword(paste0("Password for '", database_name, "':"))
    }

    dbcon <- dbConnect(drv,
                       dbname = dbname,
                       host = host,
                       port = port,
                       user = user,
                       password = password,
                       timezone = timezone)
    cat("Success!\n")

    if (hopReg){
      id <- uuid::UUIDgenerate()

      pkg <- attributes(class(drv))$package
      libraries <- list("connections")
      if (!is.null(pkg)) libraries <- c(libraries, pkg)

      meta_data <- list(
        name = database_name,
        host = host,
        port = port,
        user = user,
        timezone = timezone,
        libraries = libraries,
        type = as.character(class(dbcon))
      )

      conn_session_set(id, meta_data)

      hopRegister(dbcon, host, database_name, id, type)

      # # switch back to environment pane
      # Sys.sleep(0.1)
      # executeCommand("activateEnvironment")
    }

    return(dbcon)
  },
  error=function(cond) {
    message("\nUnable to connect: ", cond$message,  "\n")
  })
}

#' @keywords internal
check_ambig_table_name = function(tbl_name, mdc) {
  schemas = unique(mdc %>% filter(table_name == "capture") %>% pull(table_schema))
  if (length(schemas) > 1) {
    stop(paste0("Multiple tables named '", tbl_name, "' found, in schemas: ", paste(schemas, collapse = ", "), ".\n\tResults are ambiguous. Try filtering metadata_columns to the schema of interest.\n"))
  }
}

#' @keywords internal
check_ambig_table_name = function(tbl_name, mdc) {
  schemas = unique(mdc %>% filter(table_name == "capture") %>% pull(table_schema))
  if (length(schemas) > 1) {
    stop(paste0("Multiple tables named '", tbl_name, "' found, in schemas: ", paste(schemas, collapse = ", "), ".\n\tResults are ambiguous. Try filtering metadata_columns to the schema of interest.\n"))
  }
}

#' Table primary key
#'
#' Identify the primary key column(s) for a given table in the database metadata. A table's primary key caries a unique and not-null constraint, and is used to uniquely identify the rows in the table. There is only one primary key per table, usually (though not necessarily) single column.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns comprising the primary key for the table provided.
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema == "survey_data") %>%
#'             collect()
#'
#' survey_pkey <- tbl_pkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_pkey = function(tbl_name, metadata_columns) {
  check_ambig_table_name(tbl_name, metadata_columns)

  metadata_columns %>%
    filter(table_name == tbl_name,
           primary_key) %>%
    pull("column_name")
}

#' Table foreign key
#'
#' Identify the foreign key column(s) for a given table in the database metadata. A foreign key points to and mirrors the primary key of another table, establishing relationships between tables. There may be multiple foreign keys in a given table, usually (though not necessarily) single column per key.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with foreign key status in the table provided
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema =="survey_data") %>%
#'             collect()
#'
#' survey_fkey <- tbl_fkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_fkey = function(tbl_name, metadata_columns) {
  check_ambig_table_name(tbl_name, metadata_columns)
  metadata_columns %>%
    filter(table_name == tbl_name,
           foreign_key) %>%
    pull("column_name")
}

#' Table foreign key reference columns
#'
#' Identify the foreign key reference column for a given foreign key.
#' @param tbl_schema The name of the schema of the table of interest (string)
#' @param tbl_name The name of the table of interest (string)
#' @param tbl_fkey_col The name of the fkey column in the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with foreign key status in the table provided
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema =="survey_data") %>%
#'             collect()
#'
#' survey_fkey <- tbl_fkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_fkey_ref = function(tbl_schema, tbl_name, tbl_fkey_col, metadata_columns) {

  as.list(metadata_columns %>%
    filter(table_schema == tbl_schema,
           table_name == tbl_name,
           column_name == tbl_fkey_col) %>%
    select(fkey_ref_schema,
           fkey_ref_table,
           fkey_ref_column) %>%
    collect())
}


#' Table natural key
#'
#' Identify the natural key column(s) for a given table in the database metadata.  A natural key is comprised of the columns which naturally identify a given row, and which are generally used to generate ID columns used as primary keys. There is only one natural key per table, though it may be comprised of multiple columns (a "composite" key).
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The name(s) of the columns with natural key status in the table provided
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema == "survey_data") %>%
#'             collect()
#'
#' survey_nkey <- tbl_nkey('survey', mdc)
#' @importFrom dplyr %>% filter pull
#' @export
tbl_nkey = function(tbl_name, metadata_columns) {
  check_ambig_table_name(tbl_name, metadata_columns)
  metadata_columns %>%
    filter(table_name == tbl_name,
           natural_key) %>%
    pull("column_name")
}

#' All table keys
#'
#' Identify primary, foreign, and natural key columns for a given table.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @return The unique name(s) of the columns with primary, foreign, or natural key status in the table provided. If a column has multiple statuses, it will show up only once.
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema == "survey_data") %>%
#'             collect()
#'
#' survey_keys <- tbl_keys('survey', mdc)
#'
#' # collect table with all key columns from DB
#' db_survey <- tbl(dbcon, "survey") %>%
#'                 select(all_of(survey_keys)) %>%
#'                 collect()
#' @export
tbl_keys = function(tbl_name, metadata_columns) {
  return(unique(c(
    tbl_pkey(tbl_name, metadata_columns=metadata_columns),
    tbl_nkey(tbl_name, metadata_columns=metadata_columns),
    tbl_fkey(tbl_name, metadata_columns=metadata_columns)
  )))
}
