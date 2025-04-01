library(httr)
library(readr)
library(stringr)
library(lubridate)
library(jsonlite)
library(ckanr)

# function to clean text
clean_text <- function(text) {
  if (is.na(text) || is.null(text)) {
    return("")
  }
  
  # convert text to ASCII keep hyphens and punctuations
  text <- iconv(text, to = "ASCII//TRANSLIT", sub = "")
  # esc special characters
  text <- gsub('([\\\\"])', "\\\\\\1", text)
  # new lines
  text <- gsub("[\r\n\t]", " ", text)
  # weird characters from paste
  text <- gsub("[[:cntrl:]]", "", text)
  
  return(text)
}

# fetch and check for existing datasets
fetch_and_check_datasets <- function(api_key, ckan_url, dataset_title = NULL, dataset_name = NULL) {
  
  # conn
  ckanr_setup(url = ckan_url, key = api_key)
  
  # list structure
  existing_datasets <- list(
    names = character(0),
    titles = character(0),
    ids = list()
  )
  
  search_result <- package_search(rows = 10000)
  
  if (is.list(search_result) && !is.null(search_result$results)) {
    dataset_count <- length(search_result$results)
    cat("Found", dataset_count, "datasets\n")
    
    if (dataset_count > 0) {
      for (dataset in search_result$results) {
        name <- dataset$name
        title <- dataset$title
        id <- dataset$id
        
        existing_datasets$names <- c(existing_datasets$names, name)
        existing_datasets$titles <- c(existing_datasets$titles, title)
        existing_datasets$ids[[name]] <- id
        
        # Add normalized title for fuzzy matching
        mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", title))
        if (nchar(mod_title) > 0) {
          existing_datasets$ids[[paste0("title_", mod_title)]] <- id
        }
      }
    }
  }
  
  # compares a single dataset title (for dataset create)
  if (!is.null(dataset_title) || !is.null(dataset_name)) {
    existence_result <- list(
      exists = FALSE,
      id = NULL,
      match_type = NA
    )
    
    # check name
    if (!is.null(dataset_name) && dataset_name %in% existing_datasets$names) {
      existence_result$exists <- TRUE
      existence_result$id <- existing_datasets$ids[[dataset_name]]
      existence_result$match_type <- "name"
    }
    # check title
    else if (!is.null(dataset_title) && !existence_result$exists) {
      # title identical match
      title_index <- which(existing_datasets$titles == dataset_title)
      if (length(title_index) > 0) {
        existing_name <- existing_datasets$names[title_index[1]]
        existence_result$exists <- TRUE
        existence_result$id <- existing_datasets$ids[[existing_name]]
        existence_result$match_type <- "title"
      }
      # title fuzzy match
      else {
        mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", dataset_title))
        if (nchar(mod_title) > 0) {
          key <- paste0("title_", mod_title)
          if (!is.null(existing_datasets$ids[[key]])) {
            existence_result$exists <- TRUE
            existence_result$id <- existing_datasets$ids[[key]]
            existence_result$match_type <- "mod_title"
          }
        }
      }
    }
    
    return(list(
      datasets = existing_datasets,
      existence_check = existence_result
    ))
  }
  
  return(list(
    datasets = existing_datasets,
    existence_check = NULL
  ))
}

# create slug
create_slug <- function(title, existing_slugs = c()) {
  # convert to lowercase replace special characters with -
  slug <- tolower(gsub("[^a-zA-Z0-9-]", "-", title))
  
  # fix multiple dashes/long dash
  slug <- gsub("-+", "-", slug)
  slug <- gsub("^-|-$", "", slug)
  
  # limit 100
  if (nchar(slug) > 100) {
    slug <- substr(slug, 1, 100)
    # check end
    slug <- gsub("-$", "", slug)
  }
  
  # unique slug if exists
  original_slug <- slug
  counter <- 1
  
  while (slug %in% existing_slugs) {
    # add number at end
    end <- paste0("-", counter)
    # keep length 100
    prefix_length <- min(100 - nchar(end), nchar(original_slug))
    slug <- paste0(substr(original_slug, 1, prefix_length), end)
    counter <- counter + 1
  }
  
  return(slug)
}

# create dataset and upload resources
upload_datasets_and_resources <- function(datasets_csv_path, resources_csv_path, api_key, ckan_url) {
  # check for existing
  fetch_result <- fetch_and_check_datasets(api_key, ckan_url)
  existing_data <- fetch_result$datasets
  
  # read csv
  datasets <- read_csv(datasets_csv_path)
  resources <- read_csv(resources_csv_path)
  
  # create df
  dataset_results <- data.frame(
    original_id = character(),
    title = character(),
    ckan_id = character(),
    status = character(),
    error_message = character(),
    upload_time = character(),
    stringsAsFactors = FALSE
  )
  
  resource_results <- data.frame(
    original_id = character(),
    dataset_id = character(),
    name = character(),
    ckan_id = character(),
    status = character(),
    error_message = character(),
    upload_time = character(),
    stringsAsFactors = FALSE
  )
  
  created_datasets <- list()
  
  # counters for print
  total_datasets <- nrow(datasets)
  fin_datasets <- 0
  successful_datasets <- 0
  failed_datasets <- 0
  skipped_datasets <- 0
  
  total_resources <- nrow(resources)
  processed_resources <- 0
  successful_resources <- 0
  failed_resources <- 0
  
  # time
  start_time <- Sys.time()
  
  # process each dataset
  for (i in 1:nrow(datasets)) {
    dataset <- datasets[i, ]
    
    # get ID and Title
    dataset_id <- as.character(if ("ID" %in% colnames(dataset)) dataset[["ID"]] else i)
    dataset_title <- as.character(if ("Title" %in% colnames(dataset)) dataset[["Title"]] else paste("Dataset", dataset_id))
    
    # print reporting
    fin_datasets <- fin_datasets + 1
    cat("\n")
    cat("----------------------------------------\n")
    cat("PROGRESS:\n")
    cat("Processing dataset", fin_datasets, "of", total_datasets, "\n")
    cat("Dataset:", dataset_title, "\n")
    cat("----------------------------------------\n")
    
    # skip if already created
    if (dataset_id %in% names(created_datasets)) {
      skipped_datasets <- skipped_datasets + 1
      
      # reporting
      dataset_results <- rbind(dataset_results, data.frame(
        original_id = dataset_id,
        title = dataset_title,
        ckan_id = created_datasets[[dataset_id]],
        status = "Skipped",
        error_message = "Already created in this session",
        upload_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        stringsAsFactors = FALSE
      ))
      
      next
    }
    
    ##### title #####
    dataset_title <- clean_text(dataset_title)
    
    ##### slug #####
    dataset_name <- create_slug(dataset_title, existing_data$names)
    
    # check if dataset exists
    check_result <- fetch_and_check_datasets(api_key, ckan_url, dataset_title, dataset_name)
    existence_check <- check_result$existence_check
    
    if (existence_check$exists) {
      
      # get existing ckan id
      existing_id <- existence_check$id
      if (!is.null(existing_id)) {
        created_datasets[[dataset_id]] <- existing_id
        
        # reporting
        dataset_results <- rbind(dataset_results, data.frame(
          original_id = dataset_id,
          title = dataset_title,
          ckan_id = existing_id,
          status = paste0("Existing (", existence_check$match_type, " match)"),
          error_message = "",
          upload_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          stringsAsFactors = FALSE
        ))
        
        skipped_datasets <- skipped_datasets + 1
        
        # skips resource
        next
      } 
    }
    
    ##### description #####
    description <- ""
    if ("Description" %in% colnames(dataset) && !is.na(dataset[["Description"]])) {
      description <- clean_text(dataset[["Description"]])
    }
    
    #### tags #####
    tags <- list()
    tag_list <- strsplit(dataset[["Tags"]], ",")[[1]]
    for (tag in tag_list) {
      tag <- trimws(tag)
      tag <- clean_text(tag)
      if (tag != "") {
        # ckan format
        tags <- append(tags, list(list(name = tag)))
      }
    }
    
    ##### organization #####
    organization <- NULL
    if ("Organization" %in% colnames(dataset) && !is.na(dataset[["Organization"]])) {
      organization <- clean_text(dataset[["Organization"]])
    }
    
    ##### license #####
    license_id <- "notspecified"
    if ("License" %in% colnames(dataset) && !is.na(dataset[["License"]])) {
      # map license names
      license_map <- list(
        "Open Data Commons Attribution License" = "odc-by",
        "Creative Commons Attribution" = "cc-by",
        "Public Domain" = "odc-pddl",
        "Creative Commons Attribution Share-Alike" = "cc-by-sa"
      )
      
      license_name <- clean_text(dataset[["License"]])
      if (license_name %in% names(license_map)) {
        license_id <- license_map[[license_name]]
      } else {
        license_id <- license_name
      }
    }
    
    ##### metadata #####
    body <- list(
      name = dataset_name,
      title = dataset_title,
      notes = description
    )
    
    if (!is.null(organization) && !is.na(organization) && organization != "") {
      body$owner_org <- organization
    }
    
    if (license_id != "notspecified") {
      body$license_id <- license_id
    }
    
    if (length(tags) > 0) {
      body$tags <- tags
    }
    
    # convert to json - fixes special chars issue in desc
    body_json <- toJSON(body, auto_unbox = TRUE, na = "null", pretty = TRUE)
    
    ##### create dataset #####
    cat("Creating dataset:", dataset_title, "\n")
    current_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    
    response <- POST(
      url = paste0(ckan_url, "/api/3/action/package_create"),
      add_headers("Authorization" = api_key,
                  "Content-Type" = "application/json; charset=UTF-8"),
      body = body_json,
      encode = "raw"
    )
    
    # check result
    dataset_response <- content(response)
    
    if (is.list(dataset_response) && !is.null(dataset_response$success) && dataset_response$success == TRUE) {
      # success
      ckan_dataset_id <- dataset_response$result$id
      created_datasets[[dataset_id]] <- ckan_dataset_id
      
      # add to list to prevent duplicates
      existing_data$names <- c(existing_data$names, dataset_name)
      existing_data$titles <- c(existing_data$titles, dataset_title)
      existing_data$ids[[dataset_name]] <- ckan_dataset_id
      
      # fuzzy matches
      mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", dataset_title))
      if (nchar(mod_title) > 0) {
        existing_data$ids[[paste0("title_", mod_title)]] <- ckan_dataset_id
      }
      
      successful_datasets <- successful_datasets + 1
      
      # reporting
      dataset_results <- rbind(dataset_results, data.frame(
        original_id = dataset_id,
        title = dataset_title,
        ckan_id = ckan_dataset_id,
        status = "Success",
        error_message = "",
        upload_time = current_time,
        stringsAsFactors = FALSE
      ))
      
      ##### upload resources #####
      dataset_resources <- resources[resources$Dataset_ID == dataset_id, , drop = FALSE]
      
      if (nrow(dataset_resources) > 0) {
        for (j in 1:nrow(dataset_resources)) {
          resource <- dataset_resources[j, , drop = FALSE]
          
          resource_id <- as.character(if ("ID" %in% colnames(resource)) resource[["ID"]] else paste(dataset_id, j, sep = "-"))
          resource_name <- clean_text(if ("Name" %in% colnames(resource)) resource[["Name"]] else paste("Resource", j))
          
          ##### resource - desc #####
          resource_description <- ""
          if ("Description" %in% colnames(resource) && !is.na(resource[["Description"]])) {
            resource_description <- clean_text(resource[["Description"]])
          }
          
          resource_data <- list(
            package_id = ckan_dataset_id,
            name = resource_name,
            description = resource_description
          )
          
          ##### format #####
          if ("format" %in% colnames(resource) && !is.na(resource[["format"]])) {
            resource_data$format <- clean_text(resource[["format"]])
          }
          
          # based on file type
          if ("Path" %in% colnames(resource) && !is.na(resource[["Path"]])) {
            file_path <- paste0("resources/", resource[["Path"]])
            
            # files not url link
            if (file.exists(file_path)) {
              extension <- tolower(tools::file_ext(file_path))
              mime_type <- switch(extension,
                                  "pdf" = "application/pdf",
                                  "csv" = "text/csv",
                                  "xlsx" = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  "json" = "application/json",
                                  "application/octet-stream")
              
              resource_data$upload <- upload_file(file_path, mime_type)
              
              resource_response <- POST(
                url = paste0(ckan_url, "/api/3/action/resource_create"),
                add_headers("Authorization" = api_key),
                body = resource_data,
                encode = "multipart"
              )
            } else {
              cat("  File not found:", file_path, "\n")
              next
            }
          } else if ("url" %in% colnames(resource) && !is.na(resource[["url"]])) {
            # url resouce
            resource_data$url <- clean_text(resource[["url"]])
            
            resource_data_json <- toJSON(resource_data, auto_unbox = TRUE)
            
            resource_response <- POST(
              url = paste0(ckan_url, "/api/3/action/resource_create"),
              add_headers("Authorization" = api_key,
                          "Content-Type" = "application/json"),
              body = resource_data_json,
              encode = "raw"
            )
          } else {
            # skip if not url
            next
          }
          
          # reporting
          resource_result <- content(resource_response)
          
          if (is.list(resource_result) && !is.null(resource_result$success) && resource_result$success == TRUE) {
            resource_result_id <- resource_result$result$id
            successful_resources <- successful_resources + 1
            
            resource_results <- rbind(resource_results, data.frame(
              original_id = resource_id,
              dataset_id = dataset_id,
              name = resource_name,
              ckan_id = resource_result_id,
              status = "Success",
              error_message = "",
              upload_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
              stringsAsFactors = FALSE
            ))
          } else {
            # failed
            cat("  Resource upload failed\n")
            failed_resources <- failed_resources + 1
            
            # add error message
            error_msg <- toJSON(resource_result$error)
            
            resource_results <- rbind(resource_results, data.frame(
              original_id = resource_id,
              dataset_id = dataset_id,
              name = resource_name,
              ckan_id = "",
              status = "Failed",
              error_message = error_msg,
              upload_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
              stringsAsFactors = FALSE
            ))
          }
        }
      } else {
        cat("No resources found", dataset_id, "\n")
      }
    } else {
      # dataset creation fails
      cat("Dataset creation failed\n")
      error_msg <- toJSON(dataset_response$error)
      
      # add to report
      failed_datasets <- failed_datasets + 1
      
      dataset_results <- rbind(dataset_results, data.frame(
        original_id = dataset_id,
        title = dataset_title,
        ckan_id = "",
        status = "Failed",
        error_message = error_msg,
        upload_time = current_time,
        stringsAsFactors = FALSE
      ))
      
      # write csv
      write_csv(dataset_results, paste0("datasets_error_report_", format(Sys.time(), "%Y%m%d%H%M"), ".csv"))
    }
  }
  
  # generate and export reports
  timestamp <- format(Sys.time(), "%Y%m%d%H%M")
  datasets_report_path <- paste0("datasets_report_", timestamp, ".csv")
  resources_report_path <- paste0("resources_report_", timestamp, ".csv")
  
  write_csv(dataset_results, datasets_report_path)
  write_csv(resource_results, resources_report_path)
  
  
  cat("\n\n")
  cat("==================================================\n")
  cat("UPLOAD COMPLETE\n")
  cat("==================================================\n")
  cat("DATASETS:\n")
  cat("  Total:", fin_datasets, "\n")
  cat("  Uploaded:", successful_datasets, "\n")
  cat("  Failed:", failed_datasets, "\n")
  cat("  Skipped (already exist):", skipped_datasets, "\n")
  cat("\n")
  cat("RESOURCES:\n")
  cat("  Total:", processed_resources, "\n")
  cat("  Uploaded:", successful_resources, "\n")
  cat("  Failed:", failed_resources, "\n")
  cat("==================================================\n")
  
  return(list(
    datasets = dataset_results,
    resources = resource_results,
    created_datasets = created_datasets
  ))
}

api_key <- "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJiTzRNeFQxZ1FmUzVRb3FkcHluLWQ5Ujh2bGNULTBxZlRfVXlKeGJFRTg0IiwiaWF0IjoxNzQyMjQzNzU2fQ.FlCsTEiZfd5xdJQ2IStjJszbqfP79PamMY5benOsOmw"
ckan_url <- "http://staging-resources.sipexchangebc.com"
datasets_csv_path <- "./datasets.csv"
resources_csv_path <- "./resources.csv"

# run function
results <- upload_datasets_and_resources(datasets_csv_path, resources_csv_path, api_key, ckan_url)