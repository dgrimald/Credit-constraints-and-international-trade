load_estab <-
function(year, main.source, n.registries){
  
  year.source <- paste0(main.source, "/", year, ".zip")
  temp.folder <- paste0(getwd(), "/temp")
  unzip(zipfile=year.source, exdir=temp.folder)
  temp.source <- paste(temp.folder, year, sep="/")
  using.folder <- paste0(temp.source, "/using")
  dir.create(using.folder)  

  if(year %in% c(2002:2006)){
    estab.file <- paste(temp.source,
                      grep("^Estb", list.files(temp.source), value = TRUE),
                      sep="/")
    unzip(zipfile=estab.file, exdir = using.folder)
    file.i <- list.files(using.folder, full.names=TRUE)
  }else{
    estab.file <- paste(temp.source,
                        grep("[E-e][S-s][T-t][B-b]", list.files(temp.source), value = TRUE),
                        sep="/")
    command.estab <- paste("7z x ", shQuote(estab.file), " -o", shQuote(using.folder), "", sep="")
    system(command.estab)
    file.i <- list.files(using.folder, full.names=TRUE)[1]
  }
  
  # loading into R
  data.estab <- fread(file=file.i, nrows=n.registries)
  # cleaning using folder (it always has only one file)
  unlink(file.i, recursive = FALSE)
  unlink(temp.folder, recursive = TRUE)
  data.estab
}
