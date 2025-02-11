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
#' @export
hopToDB = function(prefix = NA, timezone = NULL, hopReg = TRUE) {
  drv = dbDriver("Postgres")
  dbname_var = paste(na.omit(c(prefix, "dbname")), collapse = ".")
  host_var = paste(na.omit(c(prefix, "host")), collapse = ".")
  port_var = paste(na.omit(c(prefix, "port")), collapse = ".")
  user_var = paste(na.omit(c(prefix, "user")), collapse = ".")
  password_var = paste(na.omit(c(prefix, "password")), collapse = ".")

  # attempt to fetch dbname
  dbname = Sys.getenv(dbname_var)
  if (dbname == "") {
    stop(paste0("Specified dbname not found in .Renviron: '", dbname_var, "'"), call. = FALSE)
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

#' Identify linked reference tables
#'
#' For each foreign key in tbl_name, pull information on the associated reference table. To do this recursively use \link[ribbitrrr]{tbl_chain}
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @param return_root Do you want to return info on tbl_name as "root" in the link object (TRUE by default, set to false when not needed for faster runtime)
#' @return Returns a link object listing referenced tables and their attributes
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema == "survey_data") %>%
#'             collect()
#'
#' capture_link = tbl_link("capture", mdc)
#' @importFrom dplyr %>% filter select collect
#' @export
tbl_link = function(tbl_name, metadata_columns, return_root=TRUE) {
  link = list()

  fkey_list = tbl_fkey(tbl_name, metadata_columns)


  # pull root info
  tbl_root = metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    select(table_schema, column_name) %>%
    collect()

  # record initial table as "root"
  root = list(schema = tbl_root$table_schema,
              table = tbl_name,
              pkey = tbl_root$column_name,
              nkey = tbl_nkey(tbl_name, metadata_columns),
              fkey = fkey_list)

  if (return_root) {
    link$root = root
  }

  parents = list()

    for (ff in fkey_list) {
      ref = tbl_fkey_ref(root$schema, root$table, ff, metadata_columns)

      # identify nkeys
      nkey = tbl_nkey(ref$fkey_ref_table, metadata_columns)
      # identify fkeys
      fkey = tbl_fkey(ref$fkey_ref_table, metadata_columns)
      # save all to parents list
      parents[[ref$fkey_ref_table]] = list(schema=ref$fkey_ref_schema,
                                              table=ref$fkey_ref_table,
                                              pkey=ref$fkey_ref_column,  # assumes fkey always references pkey
                                              nkey=nkey,
                                              fkey=fkey)
    }
  link[["parents"]] = parents
  return(link)
}

#' Recursively identify linked reference tables
#'
#' For each foreign key in tbl_name, pull information on the associated reference table. Do the same for any foreign keys found in this reference table, and so on. Stops when table network is exausted. Does not search beyond any tables listed in "until". To identify one iteration of reference tables, use \link[ribbitrrr]{tbl_link}.
#' @param tbl_name The name of the table of interest (string)
#' @param metadata_columns Column metadata containing the table of interest (Data Frame)
#' @param until Name(s) of table(s) beyond which you do not want to continue link search (string or list of strings)
#' @return Returns a link object (list of all encountered referenced tables and their attributes)
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("public", "all_columns")) %>%
#'             filter(table_schema == "survey_data") %>%
#'             collect()
#'
#' capture_chain = tbl_chain("capture", mdc, until=c("region"))
#' @importFrom dplyr %>% filter select collect
#' @export
tbl_chain = function(tbl_name, metadata_columns, until=NA) {
  chain = list()
  tbl_list = list(tbl_name)  # list of tables yet to search
  tbl_remaining = TRUE

  # pull root info
  tbl_root = metadata_columns %>%
    filter(table_name == tbl_name,
           key_type == "PK") %>%
    select(table_schema, column_name) %>%
    collect()

  # record initial table as "root"
  chain$root = list(schema = tbl_root$table_schema,
                    table = tbl_name,
                    pkey = tbl_root$column_name,
                    nkey = tbl_nkey(tbl_name, metadata_columns),
                    fkey = tbl_fkey(tbl_name, metadata_columns))

  # nest in list if not already
  until = list(unlist(until))

  # recursively execute tbl_link until tbl_list is exhausted
  while (tbl_remaining) {

    # pop tbl_list
    tbl_active = tbl_list[[1]]
    tbl_list[[1]] = NULL

    # lookup active table
    link_active = tbl_link(tbl_active, metadata_columns, return_root=FALSE)

    # for each reference table found
    if (length(link_active$parents) > 0) {

      for (ll in link_active$parents) {
        if (!(ll$table %in% names(chain$parents))) {

          if (!(ll$table %in% until)) {
            # add to tbl_list, unless tbl is in "until"
            tbl_list <- append(tbl_list, ll$table)
          }

          chain$parents[[ll$table]] = ll
        }
      }
    }

    # continue while loop?
    tbl_remaining = length(tbl_list)
  }

  return(chain)
}

#' Join tables with link object
#'
#' @description
#' These functions provide a framework for automatically and recursively joining tables using link objects.
#' Link objects are generated using \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}.
#'
#' - `tbl_left_join()`: Left join tables in link object.
#' - `tbl_inner_join()`: Inner join tables in link object.
#' - `tbl_full_join()`: Full join tables in link object.
#' - `tbl_right_join()`: Right join tables in link object.
#' - `tbl_join()`: Generic join tables in link object (pass join type as parameter)
#'
#' Recursively join linked database tables following provided link object. Only primary, natural, and foreign key columns are joined by default. Specify additional columns to include in "columns"
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{hopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param join Type of join to be executed ("left", "inner", "full", or "right")
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Set to "all" to include all columns. Primary, natural, and foreign key columns are always included by default (columns = NA)
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("survey_data", "metadata_columns")) %>%
#'   filter(table_schema == "survey_data") %>%
#'   collect()
#'
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc)
#'
#' # pre-filter root table (optional)
#' db_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'          species_capture,
#'          bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#'
#' # create and filter join table
#' tbl_capture_brazil = tbl_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(location == "brazil")
#'
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#'
#' @importFrom DBI Id
#' @importFrom dplyr %>% tbl select any_of left_join full_join inner_join right_join
#' @importFrom stats na.omit
#' @aliases tbl_left_join tbl_inner_join tbl_full_join tbl_right_join tbl_join
#' @export
tbl_join = function(dbcon, link, tbl=NA, join="left", by="pkey", columns="all") {

  if (is.na(columns)) {
    select_columns = TRUE
  } else if (columns == "all") {
    select_columns = FALSE
    columns = NA
  } else {
    select_columns = TRUE
  }

  # load table if not provided
  if (is.na(tbl)[[1]]) {
    cat("Pulling", link$root$table, "... ")
    tbl = tbl(dbcon, Id(link$root$schema, link$root$table))

    # select for columns
    if (select_columns) {
      tbl = tbl %>%
        select(any_of(na.omit(unique(unlist(c(
          link$root$pkey,
          link$root$nkey,
          link$root$fkey,
          columns
        ))))))
    }

    cat("done.\n")
  }

  # for each parent in link object
  for (pp in link$parents) {
    # pull for
    tbl_next = tbl(dbcon, Id(pp$schema, pp$table))

    if (select_columns) {
      tbl_next = tbl_next %>%
        select(any_of(na.omit(unique(unlist(c(
          pp$pkey,
          pp$nkey,
          pp$fkey,
          columns
        ))))))
    }

    cat("Joining with", pp$table, "... ")

    if (join == "left") {
      tbl = tbl %>%
        left_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "full") {
      tbl = tbl %>%
        full_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "inner") {
      tbl = tbl %>%
        inner_join(tbl_next, by = c(pp[[by]]))
    } else if (join == "right") {
      tbl = tbl %>%
        right_join(tbl_next, by = c(pp[[by]]))
    } else {
      stop(join, " is not a valid join type... YET. Should it be included?")
    }

    cat("done.\n")

  }

  return(tbl)
}

#' Left join using link object
#'
#' Left join tables across a provided link object, generated using \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{hopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Set to "all" to include all columns. Primary, natural, and foreign key columns are always included by default (columns = NA)
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("survey_data", "metadata_columns")) %>%
#'   filter(table_schema == "survey_data") %>%
#'   collect()
#'
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc)
#'
#' # pre-filter root table (optional)
#' db_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'          species_capture,
#'          bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#'
#' # create and filter join table
#' tbl_capture_brazil = tbl_left_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(location == "brazil")
#'
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#' @export
tbl_left_join = function(dbcon, link, tbl=NA, by="pkey", columns="all") {
  return(tbl_join(dbcon, link, tbl=tbl, join="left", by=by, columns=columns))
}

#' Inner join using link object
#'
#' Inner join tables across a provided link object, generated using \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{hopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Set to "all" to include all columns. Primary, natural, and foreign key columns are always included by default (columns = NA)
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("survey_data", "metadata_columns")) %>%
#'   filter(table_schema == "survey_data") %>%
#'   collect()
#'
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc)
#'
#' # pre-filter root table (optional)
#' db_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'          species_capture,
#'          bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#'
#' # create and filter join table
#' tbl_capture_brazil = tbl_inner_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(location == "brazil")
#'
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#' @export
tbl_inner_join = function(dbcon, link, tbl=NA, by="pkey", columns="all") {
  return(tbl_join(dbcon, link, tbl=tbl, join="inner", by=by, columns=columns))
}

#' Full join using link object
#'
#' Full join tables across a provided link object, generated using \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{hopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Set to "all" to include all columns. Primary, natural, and foreign key columns are always included by default (columns = NA)
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("survey_data", "metadata_columns")) %>%
#'   filter(table_schema == "survey_data") %>%
#'   collect()
#'
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc)
#'
#' # pre-filter root table (optional)
#' db_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'          species_capture,
#'          bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#'
#' # create and filter join table
#' tbl_capture_brazil = tbl_full_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(location == "brazil")
#'
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#' @export
tbl_full_join = function(dbcon, link, tbl=NA, by="pkey", columns="all") {
  return(tbl_join(dbcon, link, tbl=tbl, join="full", by=by, columns=columns))
}

#' Right join using link object
#'
#' Right join tables across a provided link object, generated using \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param dbcon database connection object from \link[DBI]{dbConnect} or \link[ribbitrrr]{hopToDB}
#' @param link a link object generated from \link[ribbitrrr]{tbl_link} or \link[ribbitrrr]{tbl_chain}
#' @param tbl A lazy table object from \link[dplyr]{tbl} corresponding to the root table in the provided link object (optional). If not provided, link object root table will be pulled in its entirity. Passing your own table allows you to filter or select specific columns prior to joining, thereby avoiding pulling nonessential data. Be sure to minimally include essential columns: fkeys, and nkeys or pkeys depending on "by".
#' @param by What columns to perform join on ("pkey" or "nkey")
#' @param columns Additional columns to be included from joined tables (string or list of stings). Set to "all" to include all columns. Primary, natural, and foreign key columns are always included by default (columns = NA)
#' @return Returns a single lazy table object of all linked tables joined as specified
#' @examples
#' dbcon <- hopToDB("ribbitr")
#'
#' mdc <- tbl(dbcon, Id("survey_data", "metadata_columns")) %>%
#'   filter(table_schema == "survey_data") %>%
#'   collect()
#'
#' # generate link object
#' capture_chain = tbl_chain("capture", mdc)
#'
#' # pre-filter root table (optional)
#' db_capture = tbl(dbcon, Id("survey_data", "capture")) %>%
#'   select(all_of(tbl_keys("capture", mdc)),
#'          species_capture,
#'          bd_swab_id) %>%
#'   filter(!is.na(bd_swab_id))
#'
#' # create and filter join table
#' tbl_capture_brazil = tbl_right_join(dbcon, capture_chain, tbl=db_capture) %>%
#'   filter(location == "brazil")
#'
#' # collect (pull) data from database
#' capture_brazil = tbl_capture_brazil %>%
#'   collect()
#' @export
tbl_right_join = function(dbcon, link, tbl=NA, by="pkey", columns="all") {
  return(tbl_join(dbcon, link, tbl=tbl, join="right", by=by, columns=columns))
}
