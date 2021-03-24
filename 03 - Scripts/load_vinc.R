load_vinc <-
function(year, main.source, n.registries=10){
  
  year.source <- paste0(main.source, "/", year, ".zip")
  temp.folder <- paste0(getwd(), "/temp")
  unzip(zipfile=year.source, exdir=temp.folder)
  temp.source <- paste(temp.folder, year, sep="/")
  using.folder <- paste0(temp.source, "/using")
  dir.create(using.folder)  

  if(year==2018){
    folder <- grep("using", list.dirs(temp.source, recursive = FALSE), value = TRUE, invert=TRUE)
    vinc.files <- list.files(folder, full.names =TRUE)   
  }else{
    vinc.files <- paste(temp.source,
                        grep("^[A-Z][A-Z][0-9][0-9][0-9][0-9]", list.files(temp.source), value = TRUE),
                        sep="/")
  }
  
  vinc.files <- vinc.files
  command.vinc.i <- paste("7z x ", shQuote(vinc.files), " -o", shQuote(using.folder), "", sep="")
  lapply(command.vinc.i, system)
  file.i <- list.files(using.folder, full.names=TRUE)
  
  # loading into R
  data.list <- lapply(file.i, fread, nrows=n.registries)
  data.vinc <- rbindlist(data.list)
  # cleaning using folder (it always has only one file)
  unlink(file.i, recursive = FALSE)
  unlink(temp.folder, recursive = TRUE)
  data.vinc
}
