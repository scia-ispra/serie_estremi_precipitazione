library("sf")
library("tidyverse")
library("regioniItalia")

#trova le regioni della rete dell'aeronautica
trovaRegioniAeronautica<-function(ana){
  
  
  
  #regioni<-c("valleaosta","piemonte","lombardia","pa_trento","pa_bolzano","friuliveneziagiulia","veneto","liguria")
  regioni<-ls("package:regioniItalia")
  
  ana[grep("aeronautica",ana$id),]->anaAero
  
  st_as_sf(anaAero,coords=c("Longitude","Latitude"),crs=4326)->sfAna
  st_transform(sfAna,crs=32632)->utmAna
  
  purrr::map(regioni[-grep("italia",regioni)],.f=function(nomeRegione){
    
    utils::getAnywhere(nomeRegione)->sh_regione
    sh_regione$objs[[1]]->oggetto
    st_buffer(oggetto,dist = 4000)->oggetto
    
    st_intersection(oggetto,utmAna) %>%
      mutate(regione=ifelse(grepl("aeronautica",regione),tolower(as.character(DEN_REG)),regione))->intersezione
    
    st_transform(intersezione,crs=4326)->intersezione
    intersezione$Longitude<-st_coordinates(intersezione)[,c(1)]
    intersezione$Latitude<-st_coordinates(intersezione)[,c(2)]
    
    st_geometry(intersezione)<-NULL
    
    if(nrow(intersezione)==0) return()
    
    
    intersezione
    
    
  }) %>% compact %>% bind_rows()->dfAero
  

  
  purrr::map2(.x=c("aeronautica_6710","aeronautica_6711","aeronautica_6858","aeronautica_6750","aeronautica_6691"),.y=c("lombardia","piemonte","basilicata","toscana","veneto"),.f=function(.x,.y){
    
    which(dfAero$id==.x & dfAero$regione==.y)
    
  }) %>% compact %>% unlist()->righe
  
  
  dfAero[-righe,]->dfAero
  
  if(nrow(anaAero)!=nrow(dfAero)) browser()
  
  
  bind_rows(ana[-grep("aeronautica",ana$id),],dfAero)[,c("id","SiteName","regione","Longitude","Latitude","Elevation")]
  
  
  
}#fine


checkAna<-function(ana){
  
  stopifnot(which(is.na(ana$regione))!=0)
  stopifnot(which(is.na(ana$SiteName))!=0)
  stopifnot(which(is.na(ana$Longitude))!=0)
  stopifnot(which(is.na(ana$Latitude))!=0)
  stopifnot(which(is.na(ana$Elevation))!=0)
  
}

fixnamesAna<-function(ana){
  
  ana %>%
    mutate(regione2=regione) %>%
    mutate(regione2=ifelse(regione2=="emiliaromagna"| regione2=="emilia-romagna","Emilia Romagna",regione2)) %>%
    mutate(regione2=ifelse(grepl("friuli",regione2),"Friuli Venezia Giulia",regione2))%>%
    mutate(regione2=ifelse(regione2=="valledaosta" | regione2=="valle d'aosta","Valle d'Aosta",regione2)) %>%
    mutate(regione2=str_to_title(str_to_lower(regione2))) %>%
    mutate(SiteName=str_to_title(str_to_lower(SiteName)))
  
}



read_delim("anagrafica.prcp.csv",delim=";",col_names = TRUE) %>%
  mutate(id=paste0(regione,"_",SiteID))->ana
trovaRegioniAeronautica(ana)->newAna
checkAna(newAna)
fixnamesAna(newAna)->newAna
write_delim(newAna,"fixed_anagrafica.prec.csv",delim=";",col_names = TRUE)