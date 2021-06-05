library(BiocManager)
library(tercen)
library(dplyr)
library(flowCore)
library(flowWorkspace)
library(stringr)

fcs_to_data = function(path, display_name="",
                       comp="false", comp_df=NULL,
                       transform="none",
                       discard="false") {

  indexed_flowdata = read.csv(filename, check.names=FALSE)

  # Perform transformation if needed
  if (transform == "biexponential") {
    trans_f = flowWorkspace::flowjo_biexp()
    trans_flow_data = indexed_flowdata %>% select(-contains(c('Index', 'TIME')))
    
    for (c in colnames(trans_flow_data)) {
      indexed_flowdata[, c] = trans_f(indexed_flowdata[, c])
    }
  }
  
  data_fcs = flowFrame(exprs=as.matrix(indexed_flowdata %>% select(-Index)))
  
  # Perform compensation
  if (comp == "true") {
    if (is.null(comp_df)) {
      data_fcs = compensate(data_fcs, spillover(data_fcs)$SPILL)
    } else {
      subset = indexed_flowdata %>% select(-contains(c('TIME', 'FSC', 'SSC', 
                                                       'BSC', 'Index')))
      print(colnames(subset))
      print(colnames(comp_df))
      colnames(comp_df) = colnames(subset)
      data_fcs = compensate(data_fcs, compensation(as.matrix(comp_df)))
    }
  }
  
  # Final DF
  data_fcs = as.data.frame(exprs(data_fcs))
  
  # Add index
  data_fcs = data_fcs %>% mutate(Index = indexed_flowdata$Index)
  
  # Remove unsorted events if needed
  if (discard == "true") {
    data_fcs = data_fcs %>% na_if("") %>% na.omit(Index)
  }
  
  #names_parameters = data_fcs@parameters@data$desc
  #data = as.data.frame(exprs(data_fcs))
  #col_names = colnames(data)
  #names_parameters = ifelse(is.na(names_parameters),col_names,names_parameters)
  #colnames(data) = names_parameters
  
  data_fcs %>%
    mutate_if(is.logical, as.character) %>%
    mutate_if(is.integer, as.double) %>%
    mutate(.ci = rep_len(0, nrow(.))) %>%
    mutate(filename = rep_len(basename(display_name), nrow(.)))
}

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 

# Setup operator properties
compensation <- TRUE
if(!is.null(ctx$op.value("compensation"))) compensation <- ctx$op.value("compensation")

transformation <- "biexponential"
if(!is.null(ctx$op.value("transformation"))) transformation <- ctx$op.value("transformation")

discard <- "false"
if(!is.null(ctx$op.value("discard"))) discard <- ctx$op.value("discard")

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
  comp.names <- list.files(tmpdir, full.names = TRUE, 
                           pattern="\\.comp$", ignore.case=TRUE)
  
  fcs_files = c()
  comp_files = c()
  
  for (f in f.names) {
    f_name = gsub(pattern = "\\.csv$", "", basename(f), ignore.case=TRUE)
    comp_file = "none"
    
    for (c in comp.names) {
      c_name = gsub(pattern = "\\.comp$", "", basename(c), ignore.case=TRUE)
      if (f_name == c_name) comp_file = c_name
    }
    
    fcs_files = c(fcs_files, f)
    comp_files = c(comp_files, c)
    
  }
  
  data <- data.frame(fcs=fcs_files, comp=comp_files)
  
  display_name <- fcs_files
  
} else {
  data <- data.frame(fcs=c(filename), comp=c(""))
}

# check FCS
#if(any(!isFCSfile(f.names))) stop("Not all imported files are FCS files.")

assign("actual", 0, envir = .GlobalEnv)
task = ctx$task

#2. convert them to FCS files
data %>%
  apply(1, function(row){
    fcs = row[1]
    comp = row[2]
    
    if (comp != "none") {
      comp.df <- read.csv(comp, check.names=FALSE)[-1]
      # pass CSV compensation matrix or NULL
      data = fcs_to_data(path=fcs, display_name=fcs,
                          comp="true", comp_df=comp.df,
                          transform=transformation)
    } else {
      data = fcs_to_data(path=fcs, display_name=fcs ,
                          comp="true", transform=transformation)
    }
    
    if (!is.null(task)) {
      # task is null when run from RStudio
      actual = get("actual",  envir = .GlobalEnv) + 1
      assign("actual", actual, envir = .GlobalEnv)
      evt = TaskProgressEvent$new()
      evt$taskId = task$id
      evt$total = length(data$fcs)
      evt$actual = actual
      evt$message = paste0('processing FCS file ' , fcs)
      ctx$client$eventService$sendChannel(task$channelId, evt)
    } else {
      cat('processing FCS file ' , fcs)
    }
    data
  }) %>%
  bind_rows() %>%
  ctx$addNamespace() %>%
  ctx$save()
