library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)
library(flowWorkspace)
library(stringr)

# http://localhost:5402/admin/w/9bc1fd64ee4d8642eb4c61d22c131536/ds/2c306b03-98d1-4e4d-ab98-41ed1caa3701

options("tercen.workflowId" = "9bc1fd64ee4d8642eb4c61d22c131536")
options("tercen.stepId"     = "2c306b03-98d1-4e4d-ab98-41ed1caa3701")

fcs_to_data = function(filename, 
                       comp=FALSE, comp_df=NULL,
                       transform="none") {
  print(filename)
  print(comp_df)
  indexed_flowdata = read.csv(filename, check.names=FALSE)
  patient_id = str_split(filename, "_")[[1]][1]
  date = str_split(filename, "_")[[1]][2]
  
  # Perform transformation if needed
  if (transform == "biexponential") {
    trans_f = flowWorkspace::flowjo_biexp()
    trans_flow_data = indexed_flowdata %>% select(-contains(c('Index', 'TIME'
                                                              )))
    
    for (c in colnames(trans_flow_data)) {
      indexed_flowdata[, c] = trans_f(indexed_flowdata[, c])
    }
  }
  
  data_fcs = flowFrame(exprs=as.matrix(indexed_flowdata %>% select(-Index)))

  # Perform compensation
  if (comp) {
    if (is.null(comp_df)) {
      data_fcs = compensate(data_fcs, spillover(data_fcs)$SPILL)
    } else {
      subset = indexed_flowdata %>% select(-contains(c('TIME', 'FSC', 'SSC', 
                                                     'BSC', 'Index')))
      print(colnames(subset))
      print(colnames(comp_df))
      colnames(comp_df) = colnames(subset)
      data_fcs = compensate(data_fcs, comp_df)
    }
  }
  
  # Final DF
  data_fcs = as.data.frame(exprs(data_fcs))

  #names_parameters = data_fcs@parameters@data$desc
  #data = as.data.frame(exprs(data_fcs))
  #col_names = colnames(data)
  #names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  #colnames(data) = names_parameters
  
  data_fcs %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = rep_len(0, nrow(.))) %>%
    mutate(Index = indexed_flowdata$Index) %>%
    mutate(filename = rep_len(basename(filename), nrow(.))) %>%
    mutate(patient_id = rep_len(basename(patient_id), nrow(.))) %>%
    mutate(date = rep_len(basename(date), nrow(.)))
}

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

# Setup operator properties
compensation <- TRUE
if(!is.null(ctx$op.value("compensation"))) compensation <- ctx$op.value("compensation")

transformation <- "biexponential"
if(!is.null(ctx$op.value("transformation"))) transformation <- ctx$op.value("transformation")

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
                        pattern="\\.csv$", ignore.case=TRUE)
  csv.names <- list.files(tmpdir, full.names = TRUE, 
                          pattern="\\.comp$", ignore.case=TRUE)
  
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
#if(any(!isFCSfile(f.names))) stop("Not all imported files are FCS files.")

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task

#2. convert them to FCS files
f.names %>%
  lapply(function(filename){
    # pass CSV compensation matrix or NULL
    data = fcs_to_data(filename, 
                       comp=compensation, comp_df=comp.df,
                       transform=transformation)
  
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
