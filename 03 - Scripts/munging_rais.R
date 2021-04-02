munging_rais <-
function(year, type, registries=100){
  
  # setting the folder where all information from original RAIS files are stored
  main.source <- "C:/Users/danie/OneDrive - George Mason University/01 - datalake/RAIS/RAIS - LAI"
  
  # checking consistency for year argument
  if(!year %in% c(1994:2018)){
    stop("Please select a valid year to load RAIS data (from 1994 to 2018). The layout of the year you requested has not been incorporated to the routine yet.")
  }
  
  # checking consistency for type argument
  not.valid <- case_when(type=="ESTAB" ~ 0,
                         type=="VINC" ~ 0,
                         TRUE ~ 1)
  if(not.valid){
    stop("Please select a valid type to load RAIS data. The available options are ESTAB, VINC, or BOTH")
  }
  
  if(type=="ESTAB"){
    source("load_estab.R")
    
    # define variable to keep
    old.names <- function(year){
      if(year==1994){
        c("MUNICIPIO", "ESTOQUE", "NAT ESTB", "TIPO ESTB", "CLAS CNAE 95")
      }else if(year %in% c(1995, 1996, 1997, 1998)){
        c("MUNICIPIO", "ESTOQUE", "NAT JURID", "TIPO ESTB", "IND RAIS NEG", "CLAS CNAE 95")
      }else if(year %in% c(1999:2000)){
        c("MUNICIPIO", "ESTOQUE", "NAT JURID", "TIPO ESTB", "IND RAIS NEG", "IND SIMPLES", "IND CEI VINC", "CLAS CNAE 95")
      }else if (year==2001){
        c("MUNICIPIO", "ESTOQUE$", "NAT JURID", "TIPO ESTBL", "IND RAIS NEG", "IND SIMPLES", "IND CEI VINC", "CLAS CNAE 95")
      }else if (year %in% c(2002:2005)){
        c("Município", "Qtd Vínculos Ativos", "Natureza Jurídica", "Tipo Estab", "Ind Rais Negativa", "Ind Simples", "Ind CEI Vinculado", "CNAE 95 Classe")
      }else if (year %in% c(2006:2013)){
        c("Município", "Qtd Vínculos Ativos", "Natureza Jurídica", "Tipo Estab", "Ind Rais Negativa", "Ind Simples", "Ind CEI Vinculado", "Ind Atividade Ano", "CNAE 95 Classe", "CNAE 2.0 Classe")
      }else if (year %in% c(2014:2018)){
        c("Município", "Qtd Vínculos Ativos", "Natureza Jurídica", "Tipo Estab", "Ind Rais Negativa", "Ind Simples", "Ind CEI Vinculado", "Ind Atividade Ano", "CNAE 95 Classe", "CNAE 2.0 Classe")
      }else{
        stop("This year is not yet supported. Please incorporate a new layout to load RAIS ESTAB.")
      }
    }
    list.names.keep <- lapply(1994:2018, old.names)
    names(list.names.keep) <- 1994:2018
    
    # define new names to be attributed to these variables
    new.names <- function(year){
      if(year==1994){
        c("mun.id", "total.emp", "legal.status.1994", "type.company", "cnae95")
      }else if(year %in% c(1995, 1996, 1997, 1998)){
        c("mun.id", "total.emp", "legal.status.pos94", "type.company", "ind.neg", "cnae95")
      }else if(year %in% c(1999: 2005)){
        c("mun.id", "total.emp", "legal.status.pos94", "type.company", "ind.neg", "ind.simples", "ind.cei", "cnae95")
      }else if (year %in% c(2006:2018)){
        c("mun.id", "total.emp", "legal.status.pos94", "type.company", "ind.neg", "ind.simples", "ind.cei", "ind.active", "cnae95", "cnae20")
      }else{
        stop("This year is not yet supported. Please incorporate a new layout to load RAIS ESTAB.")
      }
    }
    list.names.input <- lapply(1994:2018, new.names)
    names(list.names.input) <- 1994:2018
    
    # load raw data
    data.i <- load_estab(year=year, main.source=main.source, n.registries=registries)
    
    # the raw data brings two different fields called "Tipo Estab". We will keep only the first one
    if(year %in% c(2002:2018)){data.i <- data.i[,-grep(TRUE, names(data.i) %in% "Tipo Estab")[2], with=FALSE]}
    
    # select and rename variables
    data.i <- select(data.i,
                     matches(list.names.keep[[as.character(year)]]))
    names(data.i) <- list.names.input[[as.character(year)]]
    
    # set a standard, keeping every year with the same set of vars and compatibilizing subset.ibge before and after 2005
    set.standard <- function(data, vars){
      missing.vars <- setdiff(vars, names(data))
      fill <- as.data.frame(matrix(nrow=nrow(data), ncol=length(missing.vars)))
      names(fill) <- missing.vars
      data <- cbind.data.frame(data, fill) %>% 
        mutate(year=year) %>% 
        select(year, mun.id, total.emp, starts_with("legal"), type.company, starts_with("ind"), starts_with("cnae"))
      data
    }
    data.i <- set.standard(data=data.i, vars=unique(unlist(list.names.input)))
    data.i$company.id <- paste("c", data.i$year, 1:nrow(data.i), sep="-") 
    data.i <- relocate(data.i, company.id)
  }
  
  if(type=="VINC"){
    source("load_vinc.R")
    
    # # bring information on price indexes from IpeaData
    # # Source: http://ipeadata.gov.br/beta3/#/dados-serie?anomapa=&ascOrder=&base=macro&busca=ipca&columnOrdering=&end=2020&fonte=&serid=PRECOS_IPCAG&skip=0&start=1980&tema=&territoriality=
    # 
    # ipca <- read.csv("../02 - Data sets/PRECOS_IPCAG.csv") %>%
    #   rename(year=ANO,
    #          ipca=VALVALOR) %>% 
    #   filter(year %in% 1994:2018) %>% 
    #   mutate(ipca.shift=ifelse(year==1994, 1, 1+ipca/100),
    #          icpa.index=ifelse(year==1994, 1, 1*cumprod(ipca.shift)))
    
    # define variable to keep
    old.names <- function(year){
      if(year %in% 1994:1998){
        paste0("^", c("Idade", "Sexo Trabalhador", "Nacionalidade", "Grau Instrução 2005-1985", "Município", "Tipo Estab", "Natureza Jurídica", "CNAE 95 Classe", "Vínculo Ativo 31/12", "Qtd Hora Contr", "Vl Remun Dezembro \\(SM\\)", "Vl Remun Média \\(SM\\)", "Mês Desligamento", "Tempo Emprego", "Tipo Vínculo"))
      }else if(year %in% 1999:2005){
        paste0("^", c("Idade", "Sexo Trabalhador", "Nacionalidade", "Grau Instrução 2005-1985", "Município", "Tipo Estab", "Natureza Jurídica", "CNAE 95 Classe", "Vínculo Ativo 31/12", "Qtd Hora Contr", "Vl Remun Dezembro Nom", "Vl Remun Média Nom", "Mês Desligamento","Tempo Emprego", "Tipo Vínculo"))
      }else if(year %in% 2006:2018){
        paste0("^", c("Idade", "Sexo Trabalhador", "Nacionalidade", "Escolaridade após 2005", "Município", "Tipo Estab", "Natureza Jurídica", "CNAE 95 Classe", "Vínculo Ativo 31/12", "Qtd Hora Contr", "Vl Remun Dezembro Nom", "Vl Remun Média Nom", "Mês Desligamento", "Tempo Emprego", "Tipo Vínculo"))
      }else{
        stop("This year is not yet supported. Please incorporate a new layout to load RAIS VINC.")
      }
    }
    list.names.keep <- lapply(1994:2018, old.names)
    names(list.names.keep) <- 1994:2018
    
    # define new names to be attributed to these variables
    new.names <- function(year){
      if(year==1994){
        c("worker.age", "worker.gender", "worker.nationality", "worker.education", "mun.id", "type.company", "legal.status.1994", "cnae95", "active.contract", "hours.hired", "wage.dec", "wage.mean", "end.month", "time.employment", "type.contract")
      }else if(year %in% 1995:2018){
        c("worker.age", "worker.gender", "worker.nationality", "worker.education", "mun.id", "type.company", "legal.status.pos1994", "cnae95", "active.contract", "hours.hired", "wage.dec", "wage.mean", "end.month", "time.employment", "type.contract")
      }else{
        stop("This year is not yet supported. Please incorporate a new layout to load RAIS VINC.")
      }
    }
    list.names.input <- lapply(1994:2018, new.names)
    names(list.names.input) <- 1994:2018
    
    # load raw data
    data.i <- load_vinc(year=year, main.source=main.source, n.registries=registries)
    
    # the raw data brings two different fields called "Tipo Estab". We will keep only the first one
    data.i <- data.i[,-grep(TRUE, names(data.i) %in% "Tipo Estab")[2], with=FALSE]
    
    # select and rename variables
    data.i <- select(data.i,
                     matches(list.names.keep[[as.character(year)]]))
    names(data.i) <- list.names.input[[as.character(year)]]
    
    # set a standard, keeping every year with the same set of vars and compatibilizing subset.ibge before and after 2005
    ## until 1998 wages were reported in units of current minimum wage. We will stardardize this variable for current RS$
    ## IpeaDATA is the source for source for the Brazilian minimum wage
    ## (http://ipeadata.gov.br/beta3/#/dados-serie?anomapa=&ascOrder=&base=macro&busca=sal%C3%A1rio%20m%C3%ADnimo&columnOrdering=&end=2021&fonte=&serid=MTE12_SALMIN12&skip=0&start=1994&tema=&territoriality=)
    set.standard <- function(data, vars, t){
      minwage <- read_csv("../02 - Data sets/Brazilian Minimum Wage.csv") %>%
        rename(ano=ANO,
               mes=MES,
               minwage=VALVALOR) %>% 
        filter(mes==12, ano==year) %>% 
        select(minwage) %>% 
        pull()
 
      missing.vars <- setdiff(vars, names(data))
      fill <- as.data.frame(matrix(nrow=nrow(data), ncol=length(missing.vars)))
      names(fill) <- missing.vars
      data <- cbind.data.frame(data, fill) %>%
        filter(wage.mean>0) %>% 
        mutate(year=year,
               time.employment=as.numeric(gsub(",", ".", time.employment)),
               wage.dec=as.numeric(gsub(",", ".", wage.dec)),
               wage.dec=case_when(year<=1998 ~ wage.dec*minwage,
                                  TRUE ~ wage.dec),
               wage.mean=as.numeric(gsub(",", ".", wage.mean)),
               wage.mean=case_when(year<=1998 ~ wage.mean*minwage,
                                   TRUE ~ wage.mean),
               wage.year=case_when(time.employment<1 ~ wage.mean,
                                   time.employment>=end.month ~ wage.mean*end.month,
                                   TRUE ~ wage.mean*time.employment),
               type.contract=as.numeric(gsub("\\{ñ", NA, type.contract)),
               cnae95 = as.numeric(cnae95),
               legal.status.1994 = as.numeric(legal.status.1994),
               legals.tatus.pos1994 = as.numeric(legal.status.pos1994)) %>%
        select(year, starts_with("worker"), mun.id, type.company, starts_with("legal.status"), cnae95, active.contract, wage.year, wage.dec, hours.hired, time.employment, type.contract)
      data
    }
    data.i <- set.standard(data=data.i, vars=unique(unlist(list.names.input)), t=year)
    data.i$employment.id <- paste("e", data.i$year, 1:nrow(data.i), sep="-") 
    data.i <- relocate(data.i, employment.id)
  }
  data.i
}
