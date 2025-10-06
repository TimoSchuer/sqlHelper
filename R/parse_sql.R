library(stringr)
library(dplyr)
library(readr)
library(progress)

#' Parse SQL dump line by line and stream to CSV file
#'
#' @param sql_file_path Path to the SQL dump file
#' @param csv_file_path Path to output CSV file
#' @param sql_keywords Vector of SQL keywords to look for (default: common DML/DDL)
#' @param start_line Line number to start processing from (for resuming)
#' @param append Whether to append to existing CSV file (TRUE for resuming)
#' @param batch_size Number of statements to collect before writing to CSV
#' @export
#' @return Number of statements processed
parse_sql_to_csv <- function(
  sql_file_path,
  csv_file_path,
  sql_keywords = c("CREATE", "INSERT", "UPDATE", "DELETE", "ALTER", "DROP"),
  start_line = 1,
  append = FALSE,
  batch_size = 1000
) {
  # Get total lines for progress bar
  total_lines <- length(readLines(sql_file_path, warn = FALSE))

  # Initialize progress bar
  pb <- progress_bar$new(
    format = "Processing SQL [:bar] :percent :eta (Line :current/:total)",
    total = total_lines - start_line + 1,
    clear = FALSE,
    width = 80
  )

  # Initialize variables
  current_statement <- ""
  in_statement <- FALSE
  in_single_quote <- FALSE
  in_double_quote <- FALSE
  statement_type <- NA_character_
  statement_id <- if (append) {
    # If appending, get the last statement_id from existing file
    if (file.exists(csv_file_path)) {
      existing <- read_csv(csv_file_path, show_col_types = FALSE)
      if (nrow(existing) > 0) max(existing$statement_id) else 0
    } else {
      0
    }
  } else {
    0
  }
  line_start <- NA_integer_
  line_number <- 0
  statements_processed <- 0

  # Batch collection
  batch_statements <- tibble(
    statement_id = integer(),
    statement_type = character(),
    sql_statement = character(),
    line_start = integer(),
    line_end = integer()
  )

  # Create CSV header if not appending
  if (!append && !file.exists(csv_file_path)) {
    write_csv(batch_statements, csv_file_path)
  }

  # Open file connection
  con <- file(sql_file_path, "r")
  on.exit(close(con))

  # Skip to start_line if resuming
  if (start_line > 1) {
    readLines(con, n = start_line - 1, warn = FALSE)
    line_number <- start_line - 1
  }

  while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line_number <- line_number + 1

    # Update progress every 1000 lines
    if ((line_number - start_line + 1) %% 1000 == 0) {
      pb$update((line_number - start_line + 1) / (total_lines - start_line + 1))
    }

    # Skip comment lines and empty lines
    if (str_detect(line, "^\\s*--") || str_detect(line, "^\\s*$")) {
      next
    }

    # Check if line starts a new SQL statement
    if (!in_statement) {
      # Look for SQL keywords at the beginning of the line
      for (keyword in sql_keywords) {
        if (
          str_detect(line, regex(paste0("^\\s*", keyword), ignore_case = TRUE))
        ) {
          in_statement <- TRUE
          statement_type <- str_extract(
            line,
            regex(paste0("^\\s*(", keyword, ")"), ignore_case = TRUE)
          ) |>
            str_trim() |>
            str_to_upper()
          line_start <- line_number
          current_statement <- line
          break
        }
      }
      next
    }

    # If we're in a statement, add the current line
    if (in_statement) {
      current_statement <- paste(current_statement, line, sep = "\n")

      # Check for quotes to handle semicolons inside strings
      chars <- str_split(line, "")[[1]]

      for (char in chars) {
        if (char == "'" && !in_double_quote) {
          in_single_quote <- !in_single_quote
        } else if (char == '"' && !in_single_quote) {
          in_double_quote <- !in_double_quote
        } else if (char == ";" && !in_single_quote && !in_double_quote) {
          # End of statement found
          statement_id <- statement_id + 1
          statements_processed <- statements_processed + 1

          # Add completed statement to batch
          batch_statements <- batch_statements |>
            add_row(
              statement_id = statement_id,
              statement_type = statement_type,
              sql_statement = str_trim(current_statement),
              line_start = line_start,
              line_end = line_number
            )

          # Write batch to CSV when batch_size is reached
          if (nrow(batch_statements) >= batch_size) {
            write_csv(batch_statements, csv_file_path, append = TRUE)
            message(sprintf(
              "Written batch: %d statements (Line %d)",
              nrow(batch_statements),
              line_number
            ))
            batch_statements <- batch_statements[0, ] # Clear batch
          }

          # Reset for next statement
          current_statement <- ""
          in_statement <- FALSE
          in_single_quote <- FALSE
          in_double_quote <- FALSE
          statement_type <- NA_character_
          line_start <- NA_integer_
          break
        }
      }
    }
  }

  # Handle last statement if file doesn't end with semicolon
  if (in_statement && str_trim(current_statement) != "") {
    statement_id <- statement_id + 1
    statements_processed <- statements_processed + 1
    batch_statements <- batch_statements |>
      add_row(
        statement_id = statement_id,
        statement_type = statement_type,
        sql_statement = str_trim(current_statement),
        line_start = line_start,
        line_end = line_number
      )
  }

  # Write remaining batch
  if (nrow(batch_statements) > 0) {
    write_csv(batch_statements, csv_file_path, append = TRUE)
    message(sprintf(
      "Written final batch: %d statements",
      nrow(batch_statements)
    ))
  }

  # Complete progress bar
  pb$update(1)

  message(sprintf(
    "Processing complete. %d statements written to %s",
    statements_processed,
    csv_file_path
  ))
  message(sprintf("Last processed line: %d", line_number))

  return(statements_processed)
}

#' Resume SQL parsing from a specific line
#'
#' @param sql_file_path Path to the SQL dump file
#' @param csv_file_path Path to output CSV file
#' @param resume_line Line number to resume from
#' @param sql_keywords Vector of SQL keywords to look for
#' @param batch_size Number of statements to collect before writing to CSV
#' @export
#' @return Number of statements processed
resume_sql_parsing <- function(
  sql_file_path,
  csv_file_path,
  resume_line,
  sql_keywords = c("CREATE", "INSERT", "UPDATE", "DELETE", "ALTER", "DROP"),
  batch_size = 1000
) {
  message(sprintf("Resuming SQL parsing from line %d", resume_line))

  return(parse_sql_to_csv(
    sql_file_path = sql_file_path,
    csv_file_path = csv_file_path,
    sql_keywords = sql_keywords,
    start_line = resume_line,
    append = TRUE,
    batch_size = batch_size
  ))
}

#' Parse SQL dump line by line and extract complete statements
#'
#' @param file_path Path to the SQL dump file
#' @param sql_keywords Vector of SQL keywords to look for (default: common DML/DDL)
#' @export
#' @return Tibble with complete SQL statements
parse_sql_linebyline <- function(
  file_path,
  sql_keywords = c("CREATE", "INSERT", "UPDATE", "DELETE", "ALTER", "DROP")
) {
  # Get total lines for progress bar
  total_lines <- length(readLines(file_path, warn = FALSE))

  # Initialize progress bar
  pb <- progress_bar$new(
    format = "Processing SQL [:bar] :percent :eta",
    total = total_lines,
    clear = FALSE,
    width = 60
  )

  # Initialize variables
  statements <- tibble(
    statement_id = integer(),
    statement_type = character(),
    sql_statement = character(),
    line_start = integer(),
    line_end = integer()
  )

  current_statement <- ""
  in_statement <- FALSE
  in_single_quote <- FALSE
  in_double_quote <- FALSE
  statement_type <- NA_character_
  statement_id <- 0
  line_start <- NA_integer_
  line_number <- 0

  # Open file connection
  con <- file(file_path, "r")
  on.exit(close(con))

  while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line_number <- line_number + 1

    # Update progress every 1000 lines
    if (line_number %% 1000 == 0) {
      pb$update(line_number / total_lines)
    }

    # Skip comment lines and empty lines
    if (str_detect(line, "^\\s*--") || str_detect(line, "^\\s*$")) {
      next
    }

    # Check if line starts a new SQL statement
    if (!in_statement) {
      # Look for SQL keywords at the beginning of the line
      for (keyword in sql_keywords) {
        if (
          str_detect(line, regex(paste0("^\\s*", keyword), ignore_case = TRUE))
        ) {
          in_statement <- TRUE
          statement_type <- str_extract(
            line,
            regex(paste0("^\\s*(", keyword, ")"), ignore_case = TRUE)
          ) |>
            str_trim() |>
            str_to_upper()
          line_start <- line_number
          current_statement <- line
          break
        }
      }
      next
    }

    # If we're in a statement, add the current line
    if (in_statement) {
      current_statement <- paste(current_statement, line, sep = "\n")

      # Check for quotes to handle semicolons inside strings
      chars <- str_split(line, "")[[1]]

      for (char in chars) {
        if (char == "'" && !in_double_quote) {
          in_single_quote <- !in_single_quote
        } else if (char == '"' && !in_single_quote) {
          in_double_quote <- !in_double_quote
        } else if (char == ";" && !in_single_quote && !in_double_quote) {
          # End of statement found
          statement_id <- statement_id + 1

          # Add completed statement to results
          statements <- statements |>
            add_row(
              statement_id = statement_id,
              statement_type = statement_type,
              sql_statement = str_trim(current_statement),
              line_start = line_start,
              line_end = line_number
            )

          # Reset for next statement
          current_statement <- ""
          in_statement <- FALSE
          in_single_quote <- FALSE
          in_double_quote <- FALSE
          statement_type <- NA_character_
          line_start <- NA_integer_
          break
        }
      }
    }
  }

  # Handle last statement if file doesn't end with semicolon
  if (in_statement && str_trim(current_statement) != "") {
    statement_id <- statement_id + 1
    statements <- statements |>
      add_row(
        statement_id = statement_id,
        statement_type = statement_type,
        sql_statement = str_trim(current_statement),
        line_start = line_start,
        line_end = line_number
      )
  }

  # Complete progress bar
  pb$update(1)

  return(statements)
}

#' Extract CREATE statements using line-by-line parsing
#'
#' @param file_path Path to the SQL dump file
#' @param statement_types Vector of CREATE statement types to extract
#' @export
#' @return Tibble with CREATE statements
extract_create_statements_efficient <- function(
  file_path,
  statement_types = c(
    "TABLE",
    "VIEW",
    "INDEX",
    "TRIGGER",
    "FUNCTION",
    "PROCEDURE"
  )
) {
  all_statements <- parse_sql_linebyline(file_path, sql_keywords = "CREATE")

  create_statements <- all_statements |>
    filter(statement_type == "CREATE") |>
    mutate(
      # Extract specific CREATE type (TABLE, VIEW, etc.)
      create_type = str_extract(
        sql_statement,
        regex(
          paste0(
            "(?<=CREATE\\s+)(",
            paste(statement_types, collapse = "|"),
            ")"
          ),
          ignore_case = TRUE
        )
      ) |>
        str_to_upper(),
      # Extract object name
      object_name = str_extract(
        sql_statement,
        regex(
          "(?<=CREATE\\s+(?:TABLE|VIEW|INDEX|TRIGGER|FUNCTION|PROCEDURE)\\s+)(\\w+|`[^`]+`)",
          ignore_case = TRUE
        )
      ) |>
        str_remove_all("`")
    ) |>
    filter(!is.na(create_type), create_type %in% statement_types)

  return(create_statements)
}

#' Extract INSERT statements using line-by-line parsing
#'
#' @param file_path Path to the SQL dump file
#' @param table_names Optional vector of table names to filter for
#' @export
#' @return Tibble with INSERT statements
extract_insert_statements_efficient <- function(file_path, table_names = NULL) {
  all_statements <- parse_sql_linebyline(file_path, sql_keywords = "INSERT")

  insert_statements <- all_statements |>
    filter(statement_type == "INSERT") |>
    mutate(
      # Extract table name
      table_name = str_extract(
        sql_statement,
        regex("(?<=INSERT\\s+INTO\\s+)(\\w+|`[^`]+`)", ignore_case = TRUE)
      ) |>
        str_remove_all("`"),
      # Extract columns (if specified)
      columns = str_extract(
        sql_statement,
        regex("(?<=\\()([^)]+)(?=\\)\\s*VALUES)", ignore_case = TRUE)
      ),
      # Count number of value rows
      value_count = str_count(
        sql_statement,
        regex("VALUES\\s*\\(", ignore_case = TRUE)
      ),
      # Extract first few values for preview
      values_preview = str_extract(
        sql_statement,
        regex("VALUES\\s*\\([^)]*\\)", ignore_case = TRUE)
      ) |>
        str_remove(regex("VALUES\\s*", ignore_case = TRUE)) |>
        str_trunc(100)
    )

  # Filter by table names if provided
  if (!is.null(table_names)) {
    insert_statements <- insert_statements |>
      filter(table_name %in% table_names)
  }

  return(insert_statements)
}
