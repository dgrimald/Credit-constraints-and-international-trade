load_estab <-
function(year, main.source, n.registries=10, output="db.table", connection, table){

  if(!output %in% c("db.table", "Rdata")){
    stop("Please review the function call. The argument output can only assume two values: db.table or Rdata")
    }
  
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
  
  # determining sectorial classification
  sector.class <- ifelse(year<=2001, "cnae", "cnae10")
  
  # loading into R
  data.i<- fread(file=file.i, nrows=n.registries)
  data.i <- munging_rais(data.i, year, "ESTAB") %>% 
    rename(total_emp = total.emp,
           legal_status_1994 = legal.status.1994,
           legal_status = legal.status.pos94,
           company_type = type.company,
           activity_index = ind.neg,
           simples_index = ind.simples,
           cei_index = ind.cei,
           mun_id = mun.id, 
           rais_sector_id = cnae95) %>%
    mutate(mun_id = as.character(mun_id),
           rais_sector_classification = sector.class) %>% 
    select(year, total_emp, legal_status, legal_status_1994, company_type, activity_index, simples_index, cei_index, mun_id, rais_sector_id, rais_sector_classification)
  data.i$company_record_id  <- paste("c", year, 1:nrow(data.i), sep="-")
  data.i <- relocate(data.i, company_record_id)

  # cleaning using folder (it always has only one file)
  unlink(file.i, recursive = FALSE)
  unlink(temp.folder, recursive = TRUE)

 # write info into data lake
  if(output=="db.table"){
    dbWriteTable(connection, table, data.i, append=TRUE)
    output <- paste("RAIS Company files for year ", year, " were inserted into table ", table)
    }else{
      output <- data.i
    }
    rm(data.i)
    gc(reset=TRUE)
    output
}
