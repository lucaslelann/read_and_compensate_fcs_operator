library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)

fcs_to_data = function(filename, comp_df=NULL) {
  data_fcs = read.FCS(filename, transformation = FALSE)
  
  # Perform compensation
  if (is.null(comp_df)) {
    data_fcs = compensate(data_fcs, spillover(data_fcs)$SPILL)
  } else {
    colnames(comp_df) = colnames(spillover(data_fcs)$SPILL)
    data_fcs = compensate(data_fcs, comp_df)
  }
  
  names_parameters = data_fcs@parameters@data$desc
  data = as.data.frame(exprs(data_fcs))
  col_names = colnames(data)
  names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  colnames(data) = names_parameters
  data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = rep_len(0, nrow(.))) %>%
    mutate(filename = rep_len(basename(filename), nrow(.)))
}

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

#1. extract files
df <- ctx$cselect()

docId = df$documentId[1]
doc = ctx$client$fileService$get(docId)
filename = tempfile()
writeBin(ctx$client$fileService$download(docId), filename)
on.exit(unlink(filename))

# unzip if archive
if(length(grep(".zip", doc$name)) > 0) {
  tmpdir <- tempfile()
  unzip(filename, exdir = tmpdir)
  f.names <- list.files(tmpdir, full.names = TRUE, 
                        pattern="\\.fcs$", ignore.case=TRUE)
  csv.names <- list.files(tmpdir, full.names = TRUE, 
                          pattern="\\.csv$", ignore.case=TRUE)
  
  if (length(csv.names) == 0) { 
    comp.df <- NULL
  } else {
    comp.df <- read.csv(csv.names[1], check.names=FALSE)[-1]
  }
  
} else {
  f.names <- filename
  comp.df <- NULL
}

# check FCS
if(any(!isFCSfile(f.names))) stop("Not all imported files are FCS files.")

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task


#2. convert them to FCS files
f.names %>%
  lapply(function(filename){
    # pass CSV compensation matrix or NULL
    data = fcs_to_data(filename, comp.df)
    
    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(f.names)
      evt$actual = actual
      evt$message = paste0('processing FCS file ' , filename)
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('processing FCS file ' , filename)
    }
    data
  }) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()
