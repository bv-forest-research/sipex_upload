library(httr)
library(jsonlite)
library(readr)
library(ckanr)

# create groups
create_ckan_groups <- function(groups_csv_path, api_key, ckan_url) {
  
  ckanr_setup(url = ckan_url, key = api_key)
  
  groups <- read_csv(groups_csv_path)
  
  group_results <- data.frame(
    name = character(),
    title = character(),
    csv_id = character(),
    status = character(),
    ckan_id = character(),
    error_message = character(),
    creation_time = character(),
    stringsAsFactors = FALSE
  )
  
  # counters
  total_groups <- nrow(groups)
  successful_groups <- 0
  failed_groups <- 0
  skipped_groups <- 0
  
  # check for existing groups
  existing_groups <- fetch_existing_groups(api_key, ckan_url)
  
  cat("==================================================\n")
  cat("CREATING GROUPS\n")
  cat("==================================================\n")
  cat("Total groups to process:", total_groups, "\n\n")
  
  
  
  for (i in 1:nrow(groups)) {
    group <- groups[i, ]
    
    group_title <- if ("Title" %in% colnames(group)) clean_text(group[["Title"]]) else paste("Group", i)
    group_description <- if ("Description" %in% colnames(group)) clean_text(group[["Description"]]) else ""
    
    csv_id <- as.character(group[["Group ID"]])
    group_slug <- clean_slug(csv_id)
    
    cat("\n----------------------------------------\n")
    cat("Processing group:", group_title, "\n")
    cat("CSV ID:", csv_id, "\n")
    cat("Group slug:", group_slug, "\n")
    
    # skip if exists
    if (group_slug %in% existing_groups$names) {
      skipped_groups <- skipped_groups + 1
      
      existing_id <- existing_groups$ids[[group_slug]]
      
      group_results <- rbind(group_results, data.frame(
        name = group_slug,
        title = group_title,
        csv_id = csv_id,
        status = "Skipped (already exists)",
        ckan_id = existing_id,
        error_message = "",
        creation_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        stringsAsFactors = FALSE
      ))
      
      next
    }
    
    # data
    group_data <- list(
      name = group_slug,
      title = group_title,
      description = group_description
    )
    
    # img
    if ("Image" %in% colnames(group) && !is.na(group[["Image"]])) {
      image_url <- clean_text(group[["Image"]])
      if (image_url != "") {
        group_data$image_url <- image_url
      }
    }
    
    # to json
    group_json <- toJSON(group_data, auto_unbox = TRUE, na = "null", pretty = TRUE)
    
    # create group
    cat("Creating group:", group_title, "\n")
    current_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    
    response <- POST(
      url = paste0(ckan_url, "/api/3/action/group_create"),
      add_headers("Authorization" = api_key,
                  "Content-Type" = "application/json; charset=UTF-8"),
      body = group_json,
      encode = "raw"
    )
    
    # result
    group_response <- content(response)
    
    if (is.list(group_response) && !is.null(group_response$success) && group_response$success == TRUE) {
      
      ckan_group_id <- group_response$result$id
      
      existing_groups$names <- c(existing_groups$names, group_slug)
      existing_groups$ids[[group_slug]] <- ckan_group_id
      
      successful_groups <- successful_groups + 1
      
      group_results <- rbind(group_results, data.frame(
        name = group_slug,
        title = group_title,
        csv_id = csv_id,
        status = "Success",
        ckan_id = ckan_group_id,
        error_message = "",
        creation_time = current_time,
        stringsAsFactors = FALSE
      ))
    } else {
      cat("Failed to create group:", group_title, "\n")
      error_msg <- if (!is.null(group_response$error)) toJSON(group_response$error) else "Unknown error"
      cat("Error:", error_msg, "\n")
      
      failed_groups <- failed_groups + 1
      
      group_results <- rbind(group_results, data.frame(
        name = group_slug,
        title = group_title,
        csv_id = csv_id,
        status = "Failed",
        ckan_id = "",
        error_message = error_msg,
        creation_time = current_time,
        stringsAsFactors = FALSE
      ))
    }
  }
  
  timestamp <- format(Sys.time(), "%Y%m%d%H%M")
  report_path <- paste0("groups_report_", timestamp, ".csv")
  write_csv(group_results, report_path)
  
  cat("\n\n")
  cat("==================================================\n")
  cat("GROUP CREATION COMPLETE\n")
  cat("==================================================\n")
  cat("Total groups processed:", total_groups, "\n")
  cat("Created:", successful_groups, "\n")
  cat("Failed:", failed_groups, "\n")
  cat("Skipped (already exist):", skipped_groups, "\n")
  cat("Report saved to:", report_path, "\n")
  cat("==================================================\n")
  
  return(group_results)
}

# fetch groups
fetch_existing_groups <- function(api_key, ckan_url) {
  
  existing_groups_list <- group_list(limit = 1000)
  
  result <- list(
    names = character(0),
    ids = list()
  )
  
  if (length(existing_groups_list) > 0) {
    for (group in existing_groups_list) {
      result$names <- c(result$names, group$name)
      result$ids[[group$name]] <- group$id
    }
  }
  
  return(result)
}

# slug
create_slug <- function(title, existing_names = character(0)) {
  # First convert to lowercase entirely
  slug <- tolower(title)
  
  # Then replace non-alphanumeric characters with hyphens
  slug <- gsub("[^a-z0-9]+", "-", slug)
  slug <- gsub("^-+|-+$", "", slug)
  original_slug <- slug
  counter <- 1
  
  while (slug %in% existing_names) {
    slug <- paste0(original_slug, "-", counter)
    counter <- counter + 1
  }
  
  return(slug)
}

# clean slug from CSV ID
clean_slug <- function(id_value) {
  if (is.na(id_value) || is.null(id_value)) return("")
  
  slug <- as.character(id_value)
  slug <- trimws(slug)
  
  # Convert to lowercase and replace non-alphanumeric with hyphens
  slug <- tolower(slug)
  slug <- gsub("[^a-z0-9]+", "-", slug)
  slug <- gsub("^-+|-+$", "", slug)
  
  return(slug)
}

# clean
clean_text <- function(text) {
  if (is.na(text) || is.null(text)) return("")
  
  text <- as.character(text)
  text <- trimws(text)
  text <- gsub("\\s+", " ", text)
  
  return(text)
}

# prod
api_key_prod <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url_prod <- "https://resources.sipexchangebc.com"

# staging
api_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url <- "http://staging-resources.sipexchangebc.com"

# local test
api_key_d <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url_d <- "http://localhost:5000/"

groups_csv_path <- "groups.csv"

groups_result <- create_ckan_groups(groups_csv_path, api_key, ckan_url)
