load_vinc <-
function(year, main.source, n.registries=10, connection, table){
  
  # checking consistency for year argument
  if(!year %in% c(1994:2018)){
    stop("Please select a valid year to load RAIS data (from 1994 to 2018). The layout of the year you requested has not been incorporated to the routine yet.")
  }
  
  # Preparing extraction of raw files
  ## setting source and temporary folders for extraction
  year.source <- paste0(main.source, "/", year, ".zip")
  temp.folder <- paste0(getwd(), "/temp")
  unzip(zipfile=year.source, exdir=temp.folder)
  temp.source <- paste(temp.folder, year, sep="/")
  using.folder <- paste0(temp.source, "/using")
  dir.create(using.folder)  
  
  ## customizing for different RAIS files structures
  if(year==2018){
    folder <- grep("using", list.dirs(temp.source, recursive = FALSE), value = TRUE, invert=TRUE)
    vinc.files <- list.files(folder, full.names =TRUE)   
  }else{
    vinc.files <- paste(temp.source,
                        grep("^[A-Z][A-Z][0-9][0-9][0-9][0-9]", list.files(temp.source), value = TRUE),
                        sep="/")
  }
  
  # defining sectorial classification
  sector.class <- ifelse(year<=2001, "cnae", "cnae10")
  
  ## extracting and defining files to load
  command.vinc.i <- paste("7z x ", shQuote(vinc.files), " -o", shQuote(using.folder), "", sep="")
  lapply(command.vinc.i, system)
  file.i <- list.files(using.folder, full.names=TRUE)
  
  # Loading files and munging raw information before dumping it into db file
  ## preparing counting for the creation of unique key for employment registry
  count.start=1
  ## looping files and dumping info into data lake
  for (i in file.i){
    data.i <- fread(i, nrows=n.registries)
    ## basic munging and stardadization 
    data.i <- munging_rais(data.i, year, type="VINC") %>% 
    ## customizing to fit rp2.db
    filter(wage.year>0) %>%
      rename(employment_wage_year = wage.year,
             employment_wage_dec = wage.dec,
             employment_active_dec = active.contract,
             employment_hours = hours.hired,
             employment_duration = time.employment,
             employment_type = type.contract,
             worker_age = worker.age,
             worker_nationality = worker.nationality,
             worker_education = worker.education,
             worker_gender = worker.gender,
             company_type = type.company,
             company_legal_status_1994 = legal.status.1994,
             company_legal_status = legal.status.pos1994,
             mun_id = mun.id, 
             rais_sector_id = cnae95) %>%
      mutate(mun_id = as.character(mun_id),
             rais_sector_classification = sector.class) %>%
      select(year, employment_wage_year, employment_wage_dec, employment_active_dec, employment_hours, employment_duration, employment_type, worker_age, worker_nationality, worker_education, worker_gender, company_type, company_legal_status_1994, company_legal_status, mun_id, rais_sector_id, rais_sector_classification)
    data.i$employment_record_id  <- paste("e", year, count.start:(count.start+nrow(data.i)-1), sep="-")
    data.i <- relocate(data.i, employment_record_id)
    # set new start for next file (keys must be consistenti within years)
    count.start <- count.start+nrow(data.i) 
    
  # write info into data lake
  dbWriteTable(connection, table, data.i, append=TRUE)
  print(paste0(i, " dumped into db file."))
  
  # remove data and clean memory before loading another file
  rm(data.i)
  gc(reset=TRUE)
  }
  
  # removing temporary files from memory
  unlink(file.i, recursive = FALSE)
  unlink(temp.folder, recursive = TRUE)
  
  print(paste0("RAIS VINC for year ", year, " dumped into db file."))
}
