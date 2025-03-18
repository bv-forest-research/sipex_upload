library(readr)
library(dplyr)
library(jsonlite)
library(httr)

setup_conn <- function(url, api_key) {
  ckanr_setup(url = url, key = api_key)
}


process_tags <- function(tags) {
  if (is.null(tags) || is.na(tags) || tags == "") {
    return(NULL)
  }
  
  # split tags
  tag_array <- strsplit(tags, ",")[[1]]
  
  # create tag list
  tag_list <- lapply(tag_array, function(tag) {
    list(name = tag)
  })
  
  return(tag_list)
}

create_dataset <- function(dataset_row, owner_org) {
  dataset_id <- dataset_row$ID
  
  # limit name len for slug
  dataset_name <- substr(dataset_name, 1, 100)
  
  dataset_title <- dataset_row$Title
  dataset_description <- dataset_row$Description
  dataset_tags <- process_tags(dataset_row$Tags)
  dataset_group <- dataset_row$Group
  
  
  # create dataset
  dataset <- package_create(
    name = dataset_name,
    title = dataset_title,
    notes = dataset_description,
    owner_org = owner_org,
    tags = dataset_tags
  )
  
  # add validation, if grp not exists
  add_dataset_to_group(dataset$id, dataset_group)
  
  return(dataset)
}


add_resource <- function(dataset_id, resource_row, resource_folder = NULL) {
  resource_name <- resource_row$Title 
  resource_path <- resource_row$Path
  
  # upload res
  resource <- resource_create(
    package_id = dataset_id,
    name = resource_name,
    upload = full_path
  )
  
  return(resource)
}


add_dataset_to_group <- function(dataset_id, group_id) {
  
  member_create(
    id = group_id,
    object = dataset_id,
    object_type = "package"
  )
  
}

# try
process_datasets_resources <- function(datasets_csv, resources_csv, ckan_url, api_key, resource_folder = NULL) {
  # ckan conn
  setup_ckan(ckan_url, api_key)
  
  # read in csv
  datasets <- read_csv(datasets_csv)
  resources <- read_csv(resources_csv)
  
  # for testing - list datasets
  created_datasets <- list()
  
  
  for (i in 1:nrow(datasets)) {
    dataset_row <- datasets[i, ]
    
    owner_org <- dataset_row$Organization
    
    # create dataset and add to grp
    dataset <- create_dataset(dataset_row, owner_org)
    
    # add resources
    # store ids
    created_datasets[[as.character(dataset_row$ID)]] <- dataset$id
    
    # find all res for the dataset
    dataset_resources <- resources %>% 
      filter(ID == dataset_row$ID)
    
    # add to dataset
    for (j in 1:nrow(dataset_resources)) {
      resource_row <- dataset_resources[j, ]
      add_resource(dataset$id, resource_row, resource_folder)
    }
  }
  
  return(created_datasets)
}

# test
datasets_csv <- "./datasets.csv"
resources_csv <- "./resources.csv"
ckan_url <- "http://staging-resources.sipexchangebc.com/"
api_key <- "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJiTzRNeFQxZ1FmUzVRb3FkcHluLWQ5Ujh2bGNULTBxZlRfVXlKeGJFRTg0IiwiaWF0IjoxNzQyMjQzNzU2fQ.FlCsTEiZfd5xdJQ2IStjJszbqfP79PamMY5benOsOmw"
resource_folder <- "/resources"
upload <- process_datasets_resources(datasets_csv, resources_csv, ckan_url, api_key, resource_folder)


# this works - simple dataset upload
body <- list(
  name = "test",
  title = "Test",
  owner_org = "94eead2b-de20-40e3-81fa-dbe1ab205c12"
)

response <- POST(
  url = "http://staging-resources.sipexchangebc.com/api/3/action/package_create",
  add_headers("Authorization" = api_key),
  body = body,
  encode = "json"
)


