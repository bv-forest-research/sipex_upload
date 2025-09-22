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

# fetch organizations and check/create if needed
fetch_check_create_organization <- function(api_key, ckan_url, org_name = NULL) {
  
  # conn
  ckanr_setup(url = ckan_url, key = api_key)
  
  # list structure
  organizations <- list(
    names = character(0),
    titles = character(0),
    ids = list()
  )
  
  # df for report
  new_orgs_report <- data.frame(
    name = character(),
    slug = character(),
    id = character(),
    created_at = character(),
    stringsAsFactors = FALSE
  )
  
  # list orgs
  org_list <- organization_list(limit = 1000)
  
  if (length(org_list) > 0) {
    for (org in org_list) {
      name <- org$name
      title <- org$title
      id <- org$id
      
      organizations$names <- c(organizations$names, name)
      organizations$titles <- c(organizations$titles, title)
      organizations$ids[[name]] <- id
      
      # mod title for fuzzy matching
      mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", title))
      if (nchar(mod_title) > 0) {
        organizations$ids[[paste0("title_", mod_title)]] <- id
      }
    }
  }
  
  if (is.null(org_name) || org_name == "") {
    return(list(
      organizations = organizations,
      new_orgs_report = new_orgs_report,
      current_org_id = NULL,
      org_result = NULL
    ))
  }
  
  # clean org name
  org_name <- clean_text(org_name)
  
  if (org_name == "") {
    return(list(
      organizations = organizations,
      new_orgs_report = new_orgs_report,
      current_org_id = NULL,
      org_result = list(
        id = NULL,
        status = "skipped"
      )
    ))
  }
  
  # create org slug
  org_slug <- create_slug(org_name, organizations$names)
  
  # check if exists
  org_exists <- FALSE
  org_id <- NULL
  match_type <- NULL
  
  # name match
  if (org_slug %in% organizations$names) {
    org_exists <- TRUE
    org_id <- organizations$ids[[org_slug]]
    match_type <- "name"
  }
  # title match
  else {
    title_index <- which(organizations$titles == org_name)
    if (length(title_index) > 0) {
      existing_name <- organizations$names[title_index[1]]
      org_exists <- TRUE
      org_id <- organizations$ids[[existing_name]]
      match_type <- "title"
    }
    # fuzzy title match
    else {
      mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", org_name))
      if (nchar(mod_title) > 0) {
        key <- paste0("title_", mod_title)
        if (!is.null(organizations$ids[[key]])) {
          org_exists <- TRUE
          org_id <- organizations$ids[[key]]
          match_type <- "fuzzy_title"
        }
      }
    }
  }
  
  # if exists return id for adding
  if (org_exists) {
    return(list(
      organizations = organizations,
      new_orgs_report = new_orgs_report,
      current_org_id = org_id,
      org_result = list(
        id = org_id,
        status = "existing"
      )
    ))
  }
  
  # if org doesn't exist crate new
  # body
  org_data <- list(
    name = org_slug,
    title = org_name
  )
  
  # convert to json
  org_data_json <- toJSON(org_data, auto_unbox = TRUE)
  
  # create org
  response <- POST(
    url = paste0(ckan_url, "/api/3/action/organization_create"),
    add_headers("Authorization" = api_key, 
                "Content-Type" = "application/json"),
    body = org_data_json,
    encode = "raw"
  )
  
  # result
  org_response <- content(response)
  
  if (is.list(org_response) && !is.null(org_response$success) && org_response$success == TRUE) {
    # if success, get id
    org_id <- org_response$result$id
    
    # add to list
    organizations$names <- c(organizations$names, org_slug)
    organizations$titles <- c(organizations$titles, org_name)
    organizations$ids[[org_slug]] <- org_id
    
    # add fuzzy key
    mod_title <- tolower(gsub("[^a-zA-Z0-9]", "", org_name))
    if (nchar(mod_title) > 0) {
      organizations$ids[[paste0("title_", mod_title)]] <- org_id
    }
    
    # new org reporting
    new_orgs_report <- rbind(new_orgs_report, data.frame(
      name = org_name,
      slug = org_slug,
      id = org_id,
      created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      stringsAsFactors = FALSE
    ))
    
    return(list(
      organizations = organizations,
      new_orgs_report = new_orgs_report,
      current_org_id = org_id,
      org_result = list(
        id = org_id,
        status = "created"
      )
    ))
  } else {
    # failed to create org error
    # reporting
    return(list(
      organizations = organizations,
      new_orgs_report = new_orgs_report,
      current_org_id = NULL,
      org_result = list(
        id = NULL,
        status = "failed"
      )
    ))
  }
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
    
    if (dataset_count > 0) {
      for (dataset in search_result$results) {
        name <- dataset$name
        title <- dataset$title
        id <- dataset$id
        
        existing_datasets$names <- c(existing_datasets$names, name)
        existing_datasets$titles <- c(existing_datasets$titles, title)
        existing_datasets$ids[[name]] <- id
        
        # mod title for fuzzy matching
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


is_url <- function(path) {
  if (is.na(path) || is.null(path) || path == "") {
    return(FALSE)
  }
  
  path <- trimws(as.character(path))
  
  url_patterns <- c(
    "^https?://",           
    "^ftp://",              
    "^ftps://",             
    "^sftp://"              
  )
  
  for (pattern in url_patterns) {
    if (grepl(pattern, path, ignore.case = TRUE)) {
      return(TRUE)
    }
  }
  
  # check for www
  if (grepl("^www\\.", path, ignore.case = TRUE)) {
    return(TRUE)
  }
  
  # detects: domain.tld, domain.tld/path, subdomain.domain.tld
  domain_patterns <- c(
    # domains with path
    "^[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/",
    # others
    "^[a-zA-Z0-9.-]+\\.(com|org|net|edu|gov|info|biz|co|io|ca|uk|de|fr|au|jp|cn|ru|br|mx|in|it|es|nl|se|no|dk|fi|pl|cz|sk|hu|ro|bg|hr|si|ee|lv|lt|gr|pt|ie|lu|mt|cy|be|at|ch|li)($|\\?|#)"
  )
  
  for (pattern in domain_patterns) {
    if (grepl(pattern, path, ignore.case = TRUE)) {
      return(TRUE)
    }
  }
  
  return(FALSE)
}



# create dataset and upload resources
upload_datasets_and_resources <- function(datasets_csv_path, resources_csv_path, api_key, ckan_url) {
  
  # org data
  org_data <- fetch_check_create_organization(api_key, ckan_url)
  organizations <- org_data$organizations
  new_orgs_report <- org_data$new_orgs_report
  
  # check for existing datasets
  fetch_result <- fetch_and_check_datasets(api_key, ckan_url)
  existing_data <- fetch_result$datasets
  
  # read csv
  datasets <- read_csv(datasets_csv_path)
  resources <- read_csv(resources_csv_path)
  
  # create df
  dataset_results <- data.frame(
    original_id = character(),
    title = character(),
    organization = character(),
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
    #if (dataset_id %in% names(created_datasets)) {
    #  skipped_datasets <- skipped_datasets + 1
    
    # reporting
    # dataset_results <- rbind(dataset_results, data.frame(
    #  original_id = dataset_id,
    #  title = dataset_title,
    #  organization = "",
    #  ckan_id = created_datasets[[dataset_id]],
    #  status = "Skipped",
    #  error_message = "Already created in this session",
    #  upload_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    #  stringsAsFactors = FALSE
    #  ))
    
    #  next
    #}
    
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
          organization = "",
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
    
    # initialize tags
    if (!exists("tag_data") || is.null(tag_data)) {
      tag_data <- list(
        tags = list(
          names = character(0),
          normalized = character(0),
          ids = list()
        ),
        new_tags_report = data.frame(
          name = character(),
          id = character(),
          original_form = character(),
          created_at = character(),
          stringsAsFactors = FALSE
        )
      )
      
      # get existing
      ckanr_setup(url = ckan_url, key = api_key)
      existing_tags <- tag_list(limit = 10000)
      
      if (length(existing_tags) > 0) {
        for (tag_item in existing_tags) {
          name <- tag_item$name
          id <- tag_item$id
          
          tag_data$tags$names <- c(tag_data$tags$names, name)
          mod_name <- tolower(gsub("[^a-zA-Z0-9]", "", name))
          tag_data$tags$normalized <- c(tag_data$tags$normalized, mod_name)
          tag_data$tags$ids[[name]] <- id
          
          # mod name for fuzzy matches
          if (nchar(mod_name) > 0) {
            tag_data$tags$ids[[paste0("fuzz_", mod_name)]] <- id
          }
        }
      }
    }
    
    if ("Tags" %in% colnames(dataset) && !is.na(dataset[["Tags"]])) {
      tag_list <- strsplit(dataset[["Tags"]], ",")[[1]]
      processed_tags <- c()
      
      for (tag_text in tag_list) {
        tag_text <- trimws(tag_text)
        tag_text <- clean_text(tag_text)
        
        if (tag_text != "") {
          
          # normalize tag
          mod_tag <- tolower(gsub("[^a-zA-Z0-9]", "", tag_text))
          
          # if exists, use existing
          if (tag_text %in% tag_data$tags$names) {
            # identical match
            tags <- append(tags, list(list(name = tag_text)))
            processed_tags <- c(processed_tags, tag_text)
          }
          # fuzzy match
          else if (mod_tag != "" && mod_tag %in% tag_data$tags$normalized) {
            # get existing matching tag
            idx <- which(tag_data$tags$normalized == mod_tag)[1]
            existing_tag <- tag_data$tags$names[idx]
            
            tags <- append(tags, list(list(name = existing_tag)))
            processed_tags <- c(processed_tags, existing_tag)
          }
          else {
            
            # else use new tag
            new_tag_data <- list(name = tag_text)
            new_tag_json <- toJSON(new_tag_data, auto_unbox = TRUE)
            
            # new tag
            tag_response <- POST(
              url = paste0(ckan_url, "/api/3/action/tag_create"),
              add_headers("Authorization" = api_key,
                          "Content-Type" = "application/json"),
              body = new_tag_json,
              encode = "raw"
            )
            
            tag_result <- content(tag_response)
            
            if (is.list(tag_result) && !is.null(tag_result$success) && tag_result$success == TRUE) {
              new_tag_id <- tag_result$result$id
              new_tag_name <- tag_result$result$name
              
              # add to tag list
              tag_data$tags$names <- c(tag_data$tags$names, new_tag_name)
              tag_data$tags$normalized <- c(tag_data$tags$normalized, mod_tag)
              tag_data$tags$ids[[new_tag_name]] <- new_tag_id
              
              # add fuzzy key
              if (mod_tag != "") {
                tag_data$tags$ids[[paste0("fuzz_", mod_tag)]] <- new_tag_id
              }
              
              # reporting
              tag_data$new_tags_report <- rbind(tag_data$new_tags_report, data.frame(
                name = new_tag_name,
                id = new_tag_id,
                original_form = tag_text,
                created_at = format(Sys.time(), "%Y-%m-%d %H:%M"),
                stringsAsFactors = FALSE
              ))
              
              tags <- append(tags, list(list(name = new_tag_name)))
              processed_tags <- c(processed_tags, new_tag_name)
            } else {
              tags <- append(tags, list(list(name = tag_text)))
              processed_tags <- c(processed_tags, tag_text)
            }
          }
        }
      }
    }
    
    ##### organization #####
    organization <- NULL
    org_id <- NULL
    if ("Organization" %in% colnames(dataset) && !is.na(dataset[["Organization"]])) {
      organization <- clean_text(dataset[["Organization"]])
      
      # check if exists or create
      org_result <- fetch_check_create_organization(api_key, ckan_url, organization)
      organizations <- org_result$organizations
      new_orgs_report <- org_result$new_orgs_report
      org_id <- org_result$current_org_id
    }
    
    ##### license #####
    license_id <- "notspecified"
    if ("License" %in% colnames(dataset) && !is.na(dataset[["License"]])) {
      # map license names
      license_map <- list(
        "Creative Commons Attribution" = "CC-BY",
        "Creative Commons CCZero" = "CC0", 
        "Creative Commons Non-Commercial" = "CC BY-NC",
        "Open Data Commons Attribution License" = "ODC-BY",
        "Other (Not open); Crown copyright (Province of British Columbia), all rights reserved" = "crown-copyright-ca",
        "Other (Attribution)" = "other-at",
        "Other (Not open)" = "other-closed", 
        "Other (Open)" = "other-open",
        "Other (Public Domain)" = "other-pd",
        "Other (Non-commercial)" = "other-nc",
        "License not specified" = "notspecified"
      )
      
      license_name <- clean_text(dataset[["License"]])
      if (license_name %in% names(license_map)) {
        license_id <- license_map[[license_name]]
      } else {
        license_id <- license_name
      }
    }
    
    ##### descriptive location ##### 
    desc_loc <- ""
    if ("Descriptive Location" %in% colnames(dataset) && !is.na(dataset[["Descriptive Location"]])) {
      desc_loc <- clean_text(dataset[["Descriptive Location"]])
    }
    
    ##### author #####
    author <- ""
    if ("Author(s)" %in% colnames(dataset) && !is.na(dataset[["Author(s)"]])) {
      author <- clean_text(dataset[["Author(s)"]])
    }
    
    ##### author contact #####
    auth_cont <- ""
    if ("Author Contact" %in% colnames(dataset) && !is.na(dataset[["Author Contact"]])) {
      auth_cont <- clean_text(dataset[["Author Contact"]])
    }
    
    ##### yr published ##### 
    publication_yr <- NULL
    if ("Year Published" %in% colnames(dataset)) {
      yr <- dataset[["Year Published"]]
      publication_yr <- as.integer(clean_text(yr))
    }
    
    ##### categories ##### 
    dataset_groups <- c()
    if ("Group" %in% colnames(dataset) && !is.na(dataset[["Group"]])) {
      group_text <- clean_text(dataset[["Group"]])
      if (group_text != "") {
        dataset_groups <- strsplit(group_text, ",")[[1]]
        dataset_groups <- trimws(dataset_groups)
      }
    }
    
    ##### metadata #####
    body <- list(
      name = dataset_name,
      title = dataset_title,
      author = author,
      author_email = auth_cont,
      notes = description,
      descriptive_location = desc_loc,
      publication_yr = publication_yr
    )
    
    if (!is.null(org_id)) {
      body$owner_org <- org_id
    }
    
    if (license_id != "notspecified") {
      body$license_id <- license_id
    }
    
    if (length(tags) > 0) {
      body$tags <- tags
    }
    
    if (author != "") {
      body$author <- author
    }
    
    if (auth_cont != "") {
      body$author_email <- auth_cont
    }
    
    if (desc_loc != "") {
      body$descriptive_location <- desc_loc
    }
    
    if (!is.null(publication_yr)) {
      body$publication_yr <- publication_yr
    }
    
    # convert to json - fixes special chars issue in desc
    body_json <- toJSON(body, auto_unbox = TRUE, na = "null", pretty = TRUE)
    
    ##### create dataset #####
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
        organization = organization,
        ckan_id = ckan_dataset_id,
        status = "Success",
        error_message = "",
        upload_time = current_time,
        stringsAsFactors = FALSE
      ))
      
      
      ##### add to groups #####
      if (length(dataset_groups) > 0) {
        for (group_name in dataset_groups) {
          if (nchar(group_name) > 0) {
            group_data <- list(
              id = group_name,
              object = ckan_dataset_id,
              object_type = "package",
              capacity = "public"
            )
            
            group_data_json <- toJSON(group_data, auto_unbox = TRUE)
            
            group_response <- POST(
              url = paste0(ckan_url, "/api/3/action/member_create"),
              add_headers("Authorization" = api_key,
                          "Content-Type" = "application/json"),
              body = group_data_json,
              encode = "raw"
            )
            
            # testing
            group_result <- content(group_response)
          }
        }
      }
      
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
          
          if ("Path" %in% colnames(resource) && !is.na(resource[["Path"]]) && resource[["Path"]] != "") {
            path_value <- resource[["Path"]]
            
            if (is_url(path_value)) {
              ##### url resource #####
              resource_data$url <- clean_text(path_value)
              
              resource_data_json <- toJSON(resource_data, auto_unbox = TRUE)
              
              cat("  Processing URL resource:", resource_name, "->", path_value, "\n")
              
              resource_response <- POST(
                url = paste0(ckan_url, "/api/3/action/resource_create"),
                add_headers("Authorization" = api_key,
                            "Content-Type" = "application/json"),
                body = resource_data_json,
                encode = "raw"
              )
              
              processed_resources <- processed_resources + 1
              
            } else {
              ##### others #####
              file_path <- paste0("resources/", path_value)
              
              if (file.exists(file_path)) {
                extension <- tolower(tools::file_ext(file_path))
                mime_type <- switch(extension,
                                    "pdf" = "application/pdf",
                                    "csv" = "text/csv",
                                    "xlsx" = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    "xls" = "application/vnd.ms-excel",
                                    "json" = "application/json",
                                    "xml" = "application/xml",
                                    "txt" = "text/plain",
                                    "zip" = "application/zip",
                                    "doc" = "application/msword",
                                    "docx" = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                    "application/octet-stream")
                
                resource_data$upload <- upload_file(file_path, mime_type)
                
                resource_response <- POST(
                  url = paste0(ckan_url, "/api/3/action/resource_create"),
                  add_headers("Authorization" = api_key),
                  body = resource_data,
                  encode = "multipart"
                )
                
                processed_resources <- processed_resources + 1
                
              } else {
                cat("File not found:", file_path, "\n")
                failed_resources <- failed_resources + 1
                next
              }
            }
          } else {
            cat("No Path specified for resource:", resource_name, "\n")
            next
          }
          
          resource_result <- content(resource_response)
          
          if (is.list(resource_result) && !is.null(resource_result$success) && resource_result$success == TRUE) {
            resource_result_id <- resource_result$result$id
            successful_resources <- successful_resources + 1
            
            cat("  ✓ Success:", resource_name, "\n")
            
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
            cat("  ✗ Failed:", resource_name, "\n")
            failed_resources <- failed_resources + 1
            
            # get error message
            error_msg <- "Unknown error"
            if (!is.null(resource_result$error)) {
              if (is.list(resource_result$error)) {
                error_msg <- toJSON(resource_result$error, auto_unbox = TRUE)
              } else {
                error_msg <- as.character(resource_result$error)
              }
            }
            
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
        cat("No resources found for dataset", dataset_id, "\n")
      }
    } else {
      # dataset creation fails
      cat("Dataset creation failed\n")
      error_msg <- if (!is.null(dataset_response$error)) toJSON(dataset_response$error) else "Unknown error"
      
      # add to report
      failed_datasets <- failed_datasets + 1
      
      dataset_results <- rbind(dataset_results, data.frame(
        original_id = dataset_id,
        title = dataset_title,
        organization = organization,
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
  orgs_report_path <- paste0("new_organizations_report_", timestamp, ".csv")
  tags_report_path <- paste0("new_tags_report_", timestamp, ".csv")
  
  write_csv(dataset_results, datasets_report_path)
  write_csv(resource_results, resources_report_path)
  write_csv(new_orgs_report, orgs_report_path)
  
  # write tag report if any new tags
  if (exists("tag_data") && !is.null(tag_data) && nrow(tag_data$new_tags_report) > 0) {
    write_csv(tag_data$new_tags_report, tags_report_path)
  } else {
    empty_tags_report <- data.frame(
      name = character(),
      id = character(),
      original_form = character(),
      created_at = character(),
      stringsAsFactors = FALSE
    )
    write_csv(empty_tags_report, tags_report_path)
  }
  
  
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
  cat("\n")
  cat("ORGANIZATIONS:\n")
  cat("  Total new organizations:", nrow(new_orgs_report), "\n")
  cat("\n")
  cat("TAGS:\n")
  cat("  New tags:", if (exists("tag_data") && !is.null(tag_data)) nrow(tag_data$new_tags_report) else 0, "\n")
  cat("==================================================\n")
  
  return(list(
    datasets = dataset_results,
    resources = resource_results,
    new_organizations = new_orgs_report,
    new_tags = if (exists("tag_data") && !is.null(tag_data)) tag_data$new_tags_report else data.frame(),
    created_datasets = created_datasets
  ))
}

# prod
api_key_prod <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url_prod <- "https://resources.sipexchangebc.com"

# staging
api_key_stag <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url_stag <- "http://staging-resources.sipexchangebc.com"

# local test
api_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
ckan_url <- "http://localhost:5000/"

datasets_csv_path <- "./datasets_b5.csv"
resources_csv_path <- "./resources_b5.csv"

# run function
results <- upload_datasets_and_resources(datasets_csv_path, resources_csv_path, api_key, ckan_url)