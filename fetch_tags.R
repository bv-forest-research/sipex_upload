library(httr)
library(readr)
library(jsonlite)

facet_url <- "https://resources.sipexchangebc.com/api/3/action/package_search?rows=0&facet.field=[%22tags%22]&facet.limit=-1"

resp <- GET(facet_url, user_agent("SIPex-tag-export-script/1.0"))
facet_result <- content(resp, as = "parsed", simplifyVector = FALSE)

# fetch every tag in the CKAN instance 
export_ckan_tags <- function(api_key, ckan_url, output_path = NULL) {
  
  cat("Fetching tags from", ckan_url, "...\n")
  
  response <- GET(
    url = paste0(ckan_url, "/api/3/action/tag_list?all_fields=true"),
    add_headers("Authorization" = api_key),
    user_agent("SIPex-tag-export-script/1.0")
  )
  
  if (status_code(response) != 200) {
    stop("Request failed with status ", status_code(response), ": ", content(response, "text", encoding = "UTF-8"))
  }
  
  result <- content(response)
  
  if (is.null(result$success) || result$success != TRUE) {
    err <- if (!is.null(result$error)) toJSON(result$error, auto_unbox = TRUE) else "Unknown error"
    stop("CKAN API error: ", err)
  }
  
  tags <- result$result
  
  if (length(tags) == 0) {
    cat("No tags found.\n")
    tags_df <- data.frame(
      id = character(),
      name = character(),
      vocabulary_id = character(),
      stringsAsFactors = FALSE
    )
  } else {
    tags_df <- do.call(rbind, lapply(tags, function(tag) {
      data.frame(
        id = if (!is.null(tag$id)) tag$id else "",
        name = if (!is.null(tag$name)) tag$name else "",
        vocabulary_id = if (!is.null(tag$vocabulary_id)) tag$vocabulary_id else "",
        stringsAsFactors = FALSE
      )
    }))
    
    # sort alphabetically by name
    tags_df <- tags_df[order(tolower(tags_df$name)), ]
  }
  
  # output path
  if (is.null(output_path)) {
    output_path <- paste0("ckan_tags_", format(Sys.time(), "%Y%m%d%H%M"), ".csv")
  }
  
  write_csv(tags_df, output_path)
  
  cat("Wrote", nrow(tags_df), "tags to", output_path, "\n")
  
  return(tags_df)
}

# ---- config ----
# prod
api_key <- "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJmdFRwWWF4akI3bDBHS1BaYlhiOUp1bTNsMzZlSGNJR3d5VHR2Qy1Hb2dNIiwiaWF0IjoxNzU4MDU0ODM1fQ.R_avMA4_9f7vssBBL5Omq7Di78QAEzm12emBGIxNmwg"
ckan_url <- "https://resources.sipexchangebc.com"

# staging
# api_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
# ckan_url <- "http://staging-resources.sipexchangebc.com"

# local test
# api_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
# ckan_url <- "http://localhost:5000/"

# ---- run ----
tags_df <- export_ckan_tags(api_key, ckan_url)

