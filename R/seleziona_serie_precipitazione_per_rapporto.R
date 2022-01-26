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

purrr::partial(config::get,file="estremi_prpc.yml")->myget
myget(value="max.na")->MAX.NA
myget(value="max.size.block.na")->MAX.SIZE.BLOCK.NA
myget(value="rle.check")->RLE.CHECK

read_delim("anagrafica.prcp.csv",delim=";",col_names = TRUE) %>%
  mutate(id=paste0(regione,"_",SiteID))->ana

list.files(pattern="^prcp.+serie_valide.csv$")->ffile

annoI<-1971
annoF<-2020
creaCalendario(annoI,annoF)->calendario

purrr::map(ffile,.f=function(nomeFile){
  
  vroom(nomeFile,delim=";",col_names = TRUE,col_types = YYMMDD_TYPE)->dati
  str_remove(names(dati),"_[0-9]$")->names(dati)

  ClimateData(dati,param = "prcp")->cdati
  aggregaCD(cdati,max.na = MAX.NA,max.size.block.na = MAX.SIZE.BLOCK.NA,rle.check = RLE.CHECK)->cmensili
  aggregaCD(cmensili,max.na = 0,max.size.block.na = 0,rle.check = FALSE,seasonal = FALSE,ignore.par = FALSE)->cannuali
  as.data.frame(cannuali) %>% filter(yy>=1971)->df
  climatologiciMensili(cmensili,yearS = 1971,yearE = 2000,max.na = 6,rle.check = FALSE,max.size.block.na = 6)->valoriClimatologiciMensili
  climatologicoAnnuale(valoriClimatologiciMensili)->valoriClimatologiciAnnuali
  janitor::remove_empty(valoriClimatologiciAnnuali,which="cols")->valoriClimatologiciAnnuali
  if(!ncol(valoriClimatologiciAnnuali)) return()
  
  climatologiciMensili(cmensili,yearS = 1991,yearE = 2020,max.na = 6,rle.check = TRUE,max.size.block.na = 6)->valoriClimatologiciMensili9120
  climatologicoAnnuale(valoriClimatologiciMensili9120)->valoriClimatologiciAnnuali9120
  janitor::remove_empty(valoriClimatologiciAnnuali9120,which="cols")->valoriClimatologiciAnnuali9120
  if(!ncol(valoriClimatologiciAnnuali9120)) return()
  
  base::intersect(names(valoriClimatologiciAnnuali),names(valoriClimatologiciAnnuali9120))->stazioniDaTestare
  base::setdiff(stazioniDaTestare,"yy")->stazioniDaTestare

  if(!length(stazioniDaTestare)) return()
  
  purrr::map(stazioniDaTestare,.f=function(codiceStazione){
    
    df[[codiceStazione]]->serie
    serie[(length(serie)-2):length(serie)]->finale
    if(all(is.na(finale))) return()
    na_kalman(serie)->serie_filled
    trend::pettitt.test(serie_filled)->risultato
    if(risultato$p.value<0.05) return()
    
    codiceStazione
    
  }) %>% compact()->listaSerieValide
  
  if(!length(listaSerieValide)) return()

  regione<-str_remove(nomeFile,"\\.serie_valide.csv$")
  regione<-str_remove(regione,"^prcp\\.")
  unlist(listaSerieValide)->codici
  dati[,codici]->temp
  names(temp)<-paste0(regione,"_",names(temp))
  
  bind_cols(dati[,c("yy","mm","dd")],temp) %>% filter(yy>=1971)

}) %>% compact()->listaDatiPerRegione

if(!length(listaDatiPerRegione)) stop("Nessun dato valido")

reduce(listaDatiPerRegione,.f=left_join,.init = calendario)->finale

#calcolo estremo r95p
purrr::map(names(finale %>% select(-yy,-mm,-dd)),.f=function(codiceStazione){
  
  
  finale[,c("yy","mm","dd",codiceStazione)] %>%
    mutate(yymmdd=glue::glue("{yy}-{mm}-{dd}")) %>%
    seplyr::rename_se(c("stazione":=codiceStazione))->df
  as.PCICt(df$yymmdd,cal="gregorian",format = "%Y-%m-%d")->calendario_per_indici
  climdex.pcic::climdexInput.raw(prec=df$stazione,prec.dates = calendario_per_indici,max.missing.days = c(annual=15,monthly=5),base.range = c(1971,2000))->climdexData
  climdex.r95ptot(climdexData)->risultato
  
  tibble(yy=annoI:annoF,indice=risultato)->dfEstremo
  
  dfEstremo %>%
    filter(yy<=2000 & yy>=1971) %>%
    filter(is.na(indice)) %>%
    nrow()->dati_mancanti_periodo_climatologico
  
  if(dati_mancanti_periodo_climatologico >6) return() #non posso calcolare il climatologico di questo indice..lo scarto
  
  dfEstremo %>%
    filter(yy<=2020 & yy>=1991) %>%
    filter(is.na(indice)) %>%
    nrow()->dati_mancanti_ultimo_periodo_climatologico
  
  if(dati_mancanti_ultimo_periodo_climatologico >6) return()
  
  if(all(is.na(risultato[(length(risultato)-2):length(risultato)]))) return()
  
  finale[,c("yy","mm","dd",codiceStazione)]
  
}) %>% compact->listaStazioniSelezionate

if(!length(listaStazioniSelezionate)) stop("Nessuna stazione selezionata")

reduce(listaStazioniSelezionate,.f=left_join,.init=calendario)->finale

st_as_sf(ana %>% filter(id %in% names(finale)),coords=c("Longitude","Latitude"),crs=4326)->sfAna

st_transform(sfAna,crs=32632)->sfAna
st_intersection(nord,sfAna)->stazioniNord
st_intersection(centro,sfAna)->stazioniCentro
st_intersection(sud,sfAna)->stazioniSud

bind_rows(stazioniNord,sfAna[sfAna$SiteName=="CAPO MELE",])->stazioniNord
bind_rows(stazioniCentro,sfAna[sfAna$SiteName=="Civitavecchia",])->stazioniCentro
bind_rows(stazioniCentro,sfAna[sfAna$SiteName=="Livorno Mareografo",])->stazioniCentro
bind_rows(stazioniSud,sfAna[sfAna$SiteName=="TERMOLI",])->stazioniSud

as.matrix(st_distance(stazioniSud))->distanzaSud
summary(as.numeric(distanzaSud[lower.tri(distanzaSud,diag = FALSE)])) #le stazioni al sud distano mediamente 25 km

elimina_vicine<-function(p1,tolerance){
  
  st_distance(p1)->distanza
  as.data.frame(as.matrix(distanza))->distanza
  names(distanza)<-p1$id
  distanza$id<-p1$id
  
  distanza %>%
    gather(key="id2",value="dis",-id) %>%
    mutate(dis=as.numeric(dis)) %>% 
    filter(dis>0 & dis<=tolerance) %>%
    arrange(id)->gdistanza
  
  if(!nrow(gdistanza)) return(p1)
  
  vet<-c()
  purrr::walk2(gdistanza$id,gdistanza$id2,.f=function(.x,.y){
    
    
    if(!(.x %in% vet)){
      vet<<-c(.y,vet)
      #return(.x)
    }
    
  })
  
  p1[! p1$id %in% vet,]
  
}#fine elimina_vicine

elimina_vicine(p1=stazioniNord,tolerance = 25000)->stazioniNordFiltrate
elimina_vicine(p1=stazioniCentro,tolerance = 25000)->stazioniCentroFiltrate
elimina_vicine(p1=stazioniSud,tolerance = 1000)->stazioniSudFiltrate


write_delim(finale[,c("yy","mm","dd",stazioniNordFiltrate$id)],"prpc_nord_serie_selezionate_per_estremi_rapporto_1991_2020.csv",delim=";",col_names = TRUE)
write_delim(finale[,c("yy","mm","dd",stazioniCentroFiltrate$id)],"prpc_centro_serie_selezionate_per_estremi_rapporto_1991_2020.csv",delim=";",col_names = TRUE)
write_delim(finale[,c("yy","mm","dd",stazioniSudFiltrate$id)],"prpc_sud_serie_selezionate_per_estremi_rapporto_1991_2020.csv",delim=";",col_names = TRUE)


ClimateData(finale[,c("yy","mm","dd",stazioniNordFiltrate$id,stazioniCentroFiltrate$id,stazioniSudFiltrate$id)],param="prcp")->cfinale
aggregaCD(cfinale,max.na = MAX.NA,max.size.block.na = MAX.SIZE.BLOCK.NA,rle.check = RLE.CHECK)->mfinale
aggregaCD(mfinale,max.na = 0,max.size.block.na = 0,ignore.par = FALSE,seasonal = FALSE,rle.check = FALSE)->afinale
as.data.frame(afinale)->afinale

vis_miss(afinale)

ana %>%
  filter(id %in% names(afinale))->subAna

which(! names(afinale) %in% ana$id)->colonne
stopifnot(length(names(afinale)[colonne])==1)

leaflet(data=subAna) %>%
  setView(lng = 12,lat=42,zoom = 6) %>%
  addTiles() %>%
  addCircleMarkers(lng=~Longitude,lat=~Latitude)
