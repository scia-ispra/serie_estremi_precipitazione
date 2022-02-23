#26 gennaio 2022
#Selezioniamo le stazioni per il nuovo rapporto indicatori (dati precipitazione)

#Prima di tutto testiamo l'omogeneità delle serie usando il test di Petit sui cumulati annuali. I cumulati vengono calcolati accettando 5 giorni mancanti anche consecutivi.
#Regole piu' severe non permettono di selezionare dati per la Puglia e la Sardegna. I cumulati mensili vengono utilizzati per costruire il climatologico  1971-2000 e il climatologico
#1991-2020. Solo le serie che hanno il climatologico su entrambi i periodi vengono testate per il test di Petit.

#I cumulati annuali vengono testati con il tes di Petit: le serie disomogenee vengono scartate.

#A questo punto vogliamo selezionare le serie omnogenee per cui possiamo calcolare i valori estremi e le rispettive anomalie 1971-2000. Calcoliamo ad esempio l'R95p.
#Per calcolare l'R95p accettiamo 5 giorni mancanti al mese per un totale di 15 giorni nell'anno. Questo significa che non tutte le serie omogenee selezionate
#andranno bene per il calcolo dei valori estremi.

#Una volta calcolato l'estremo verifichiamo di poterne calcolare il climatologico annuale nel 'periodo 1971-2000 e nel periodo 1991-2020. Inoltr verifichiamo che gli ultimi tre anni
#della serie non siano tutti NA. Se una serie supera queste condizioni viene selezionata.

#Suddividiamo le serie in nord centro e sud. Il sud ha meno serie (circa una quarantina) con una distanza media delle stazioni di circa 25km. Filtriamo in nord e il centro in modo di avere
#un numero simile di stazioni con una distanza media di 25 km.

#Il risultato finale sono tre file per cui potremo calcolare gli estremi di precipitazione per il rapporto
rm(list=objects())
library("tidyverse")
library("guido")
library("vroom")
library("janitor")
library("trend")
library("climatologici")
library("imputeTS")
library("visdat")
library("leaflet")
library("climdex.pcic")
library("PCICt")
library("regioniItalia")
library("sf")
library("seplyr")
library("config")

reduce(list(piemonte,emiliaromagna,liguria,veneto,valleaosta,trentino,friuliveneziagiulia,lombardia),.f=st_union)->nord
reduce(list(toscana,umbria,lazio,abruzzo,marche),.f=st_union)->centro
reduce(list(campania,molise,basilicata,puglia,sicilia,sardegna,calabria),.f=st_union)->sud

purrr::partial(config::get,file="climatologico.yml",config="precipitation")->myget
myget(value="max.na")->MAX.NA
myget(value="max.size.block.na")->MAX.SIZE.BLOCK.NA
myget(value="rle.check")->RLE.CHECK


read_delim("anagrafica.prcp.csv",delim=";",col_names = TRUE) %>%
  mutate(id=paste0(regione,"_",SiteID))->ana

list.files(pattern="^prcp.+serie_valide.csv$")->ffile

annoI<-1961
annoF<-2020
creaCalendario(annoI,annoF)->calendario

purrr::map(ffile,.f=function(nomeFile){
  
  str_remove(str_remove(nomeFile,"\\.serie_valide.csv$"),"prcp\\.")->regione
  
  vroom(nomeFile,delim=";",col_names = TRUE,col_types = YYMMDD_TYPE)->dati
  str_remove(names(dati),"_[0-9]$")->names(dati)
  ClimateData(dati,param = "prcp")->cdati
  aggregaCD(cdati,max.na = 0,max.size.block.na = 0,rle.check =TRUE)->cmensili
  
  #provo a calcolare il climatologico 1991-2020..tengo solo le stazioni per cui posso ottenere questo valore climatologico
  climatologiciMensili(cmensili,yearS = 1991,yearE =2020,max.na = myget(value="blocco.anni.na.climatologico"),rle.check = FALSE,max.size.block.na =  myget(value="max.blocco.anni.na.climatologico"))->valoriClimatologiciMensili
  janitor::remove_empty(as.data.frame(valoriClimatologiciMensili),which = "cols")->temp
  if(ncol(temp)==2) return()
  
  base::setdiff(names(temp),c("yy","mm"))->codiciStazioni
  
  purrr::map(codiciStazioni,.f=function(codice){
    
    dati[,c("yy","mm","dd",codice)] %>% seplyr::rename_se(c("value":=codice))->df
    ClimateData(df,param = "prpc")->cdf
    
    aggregaCD(cdf,max.na = 5,max.size.block.na = 5,rle.check = TRUE)->cmensili
    aggregaCD(cmensili,max.na = 0,max.size.block.na = 0,rle.check = TRUE,ignore.par = FALSE,seasonal = FALSE)->cannuali
    
    as.data.frame(cannuali) %>%
      filter(!is.na(value))->cannuali_senza_na
    
    if(nrow(cannuali_senza_na)<24) return() #questo in realtà nn puo' essere visto che abbiamo calcolato ilclimatologico 91-20, quindi almeno un trentennio climatologico ci sta
    min(cannuali_senza_na$yy)->annoInizioSerie
    
    cannuali[lubridate::year(zoo::index(cannuali))>=annoInizioSerie, ]->serieDaTestare
    as.data.frame(serieDaTestare)->serieDaTestare
    serieDaTestare[["value"]]->x
    na_kalman(x,maxgap = 4)->x_senza_na

    tryCatch({
      trend::pettitt.test(x_senza_na)
    },error=function(e){
      NULL
    })->risultato
    
    if(is.null(risultato)) return()
    if(risultato$p.value<=0.05) return()

    dati[,c("yy","mm","dd",codice)] %>% seplyr::rename_se(c(paste0(regione,"_",codice):=codice))
    
  }) %>% compact ->listaDati
  
  
  if(!length(listaDati)) return()
  
  reduce(listaDati,.f=left_join,.init=calendario)->datiOmogenei

  datiOmogenei
  
}) %>% compact->listaDatiOmogenei

if(!length(listaDatiOmogenei)) stop("Nessun dato valido")

reduce(listaDatiOmogenei,.f=left_join,.init=calendario)->datiOmogenei


ClimateData(datiOmogenei,param = "prcp")->cdati
aggregaCD(cdati,max.na=myget(value="max.na"),max.size.block.na = myget(value="max.size.block.na"),rle.check = TRUE)->mdati  

inizioClim<-seq(1961,1991,by=10)
fineClim<-seq(1990,2020,by=10)

purrr::map2(inizioClim,fineClim,.f=function(.x,.y){

  
  climatologiciMensili(mdati,yearS = .x,yearE = .y,max.na =  myget(value="blocco.anni.na.climatologico"),max.size.block.na = myget("max.blocco.anni.na.climatologico"),rle.check = TRUE)->mensili_climatologici
  janitor::remove_empty(as.data.frame(mensili_climatologici),which = "cols")->temp
  if(ncol(temp)==2) return(list(mensili=NULL,annuale=NULL))
  apply(temp %>%select(-yy,-mm),MARGIN = 2,FUN = sum,na.rm=FALSE)->vannuale_climatologico
  tibble(id=names(vannuale_climatologico),Annuale=vannuale_climatologico) %>%
    spread(key=id,value=Annuale) %>%
    mutate(yy=temp$yy[1]) %>%
    dplyr::select(yy,everything())->annuale_climatologico

  janitor::remove_empty(as.data.frame(annuale_climatologico),which = "cols")->temp
  if(ncol(temp)==1) return(list(mensili=as.data.frame(mensili_climatologici),annuale=NULL))

  return(list(mensili=janitor::remove_empty(as.data.frame(mensili_climatologici),which = "cols"),annuale=janitor::remove_empty(as.data.frame(annuale_climatologico),which = "cols")))
  
  
})->listaClimatologici

names(listaClimatologici)<-inizioClim
purrr::map_dfr(listaClimatologici,"mensili")->mensili
mensili %>%
  janitor::remove_empty(which="cols") %>%
  mutate(mese=month.name[mm]) %>%
  mutate(mese=factor(mese,levels=month.name,ordered=TRUE)) %>%
  select(-mm) %>%
  gather(key="id",value="climatologico",-yy,-mese)->gmensili

gmensili %>%
  group_by(yy,id) %>%
  summarise(somma=sum(climatologico,na.rm=TRUE)) %>%
  ungroup() %>%
  filter(somma<1)->stazioni_da_eliminare

spread(gmensili,key=mese,value=climatologico)->smensili
anti_join(smensili,stazioni_da_eliminare,by=c("id","yy"))->mensili_finali
write_delim(mensili_finali,"climatologici_mensili_prec_homog.csv",delim=";",col_names = TRUE)


purrr::map_dfr(listaClimatologici,"annuale")->annuali
annuali %>%
  gather(key="id",value="Annuale",-yy) %>%
  spread(key=yy,value=Annuale)->annuali_finali
write_delim(annuali_finali,"climatologici_annuali_prec_homog.csv",delim=";",col_names = TRUE)

