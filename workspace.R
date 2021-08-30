library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)
#####


options("tercen.workflowId" = "237423aa306b89c403ec93aaaa0463c7")
options("tercen.stepId"     = "eae75c16-ddf8-4ebc-af7d-afe8a2fe46d8")

getOption("tercen.workflowId")
getOption("tercen.stepId")

#####

fcs_to_data <- function(filename, comp="false", comp_df=NULL) {
  data_fcs <- read.FCS(filename, transformation = FALSE, dataset = 2,truncate_max_range = FALSE)
  
  # dataset is set to 2 for LMD files, it is ignored if there is only 1 dataset

  #In readFCSdata(con, offsets, txt, transformation, which.lines, scale,  :
  #Some data values of 'PE-Cy5-A' channel exceed its $PnR value 82897 and will be truncated!
  #To avoid truncation, either fix $PnR before generating FCS or set 'truncate_max_range = FALSE'
  
  if (comp == "true") {
    if (is.null(comp_df)) {
      if ( is.null ( data_fcs@description$SPILL) ) {
        data_fcs <- compensate(data_fcs, spillover(data_fcs)$'$SPILLOVER')
      } else {
        data_fcs <- compensate(data_fcs, spillover(data_fcs)$SPILL)
      }
      
    } else {
      subset <- indexed_flowdata %>% select(-contains(c('TIME', 'FSC', 'SSC', 
                                                       'BSC', 'Index')))
      print(colnames(subset))
      print(colnames(comp_df))
      colnames(comp_df) <- colnames(subset)
      data_fcs <-compensate(data_fcs, compensation(as.matrix(comp_df)))
    }
  }
  
  names_parameters <- data_fcs@parameters@data$desc
  data <- as.data.frame(exprs(data_fcs))
  col_names <- colnames(data)
  names_parameters <- ifelse(is.na(names_parameters),col_names,names_parameters)
  colnames(data) <- names_parameters
  
  
  data %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = rep_len(0, nrow(.))) %>%
    mutate(filename = rep_len(basename(filename), nrow(.)))
}
################

ctx <- tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required")

which.lines <- NULL
if(!is.null(ctx$op.value('which.lines')) && !ctx$op.value('which.lines') == "NULL") which.lines <- as.integer(ctx$op.value('which.lines'))

# extract files
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
  f.names <- list.files(tmpdir, full.names = TRUE)
} else {
  f.names <- filename
}

# check FCS
if(any(!isFCSfile(f.names))) stop("Not all imported files are FCS files.")

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task


# convert them to FCS files
f.names %>%
  lapply(function(filename){
    data = fcs_to_data(filename,comp="true")
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
