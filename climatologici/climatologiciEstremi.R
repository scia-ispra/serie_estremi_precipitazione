#21 febbraio: febbraio moltiplicato per 28.25 giorni

#Calcolo dei climatologgici degli estremi (estremi basati sul superamento di soglie). 
#Gli estremi vengono qui calcolati senza utilizzare Climpact: non vale la pena utilizzare Climpact2 per indici cosi semplici da calcolare, soprattutto 
#perche' Climapct2 richiede che i dati di prcp,tmax e tmin siano tutti nello stesso file. Qui invece noi vogliamo calcolare gli indici (e i relativi climatologici)
#separatamente.
rm(list=objects())
library("tidyverse")
library("guido")
library("furrr")

future::plan(multicore,workers=6)

#quale parametro: i file di input seguono lo schema: prcp.abruzzo.serie_valide.csv
#ATTENZIONE: per la precipitazione il programma assume che esista anche il file climatologici_mensili_prec_homog.csv. Il programma dopo aver calcolato un indice
#per tutte le regioni italiane legge il file dei climatologici mensili e scarta le stazioni che non sono elencate in quel file. Questo perchè vogliamo tenere
#solo le stazioni che sono risultate potenzialmente omogenee mediante il test di petitt, test effettuato quando sono stati calcolati i climatologici mensili
#di precipitazione). Se si volessero tenere tutte le stazioni, commentare a fine fale il codice che legge il file dei climatologici e fa il left_join.
PARAM<-c("tmax","tmin","prcp")[3]
list.files(pattern = "^.+\\..+serie_valide.csv$")->listaFiles

#quanti giorni NA in un mese sono tollerati? Per gli estremi la situazione è molto piu' delicata che per il calcolo dei cumulati/medie mensili.
MANCANTI_AMMESSI<-0

#Creiamo una funzione molto generica (closure) per il calcolo degli estremi. #Questa definizione molto genereica va bene per 
#gli indici di temperetaura che per gli indici di precipotazione.

#fun puo' essere `>=` o `<=`. Soglia è la soglia oltre la quale vengono conteggiati i giorni in un mese.
indice<-function(soglia,fun){
  
  function(x){
    
    fun(x,soglia)
    
  }
  
}#fine indice


#definizione di media per climatologico mensile: sono ammessi al massimo 6 NA
media<-function(x){
  
  if((length(x)!=30) && (length(x)!=20)) browser()
  
  ifelse(length(x)==30,6,0)->max_numero_na
  
  length(which(is.na(x)))->numero_na
  if(numero_na>max_numero_na) return(NA)
  mean(x,na.rm=TRUE)
  
}#fine media

if(PARAM=="prcp"){
  soglie<-c(1,5,10,20,50,100)  
  length(soglie)->numero_soglie
  confronti<-rep(">=",numero_soglie)
}else if(PARAM=="tmax"){
  soglie<-c(25,30,35,40)
  length(soglie)->numero_soglie
  confronti<-c(">",">=",">=",">=")
}else if(PARAM=="tmin"){
  soglie<-c(0,20)  
  length(soglie)->numero_soglie
  confronti<-c("<",">")
}

stopifnot(length(confronti)==numero_soglie)

ifelse(grepl(">",confronti),"gt","lt")->confronti_chr
paste0(PARAM,"_",confronti_chr,soglie)->nomiIndici #questi servono per i nomi di file di output

#listaFunzioni è una lista di 5 funzioni definite mediante la closure "indice"
purrr::map2(.x=confronti,.y=soglie,.f=~(indice(soglia=.y,fun=base::get(.x))))->listaFunzioni

#ciclo su listaFunzioni/indice estremo
furrr::future_walk(1:length(listaFunzioni),.f=function(ii){
  
  qualeIndice<-nomiIndici[ii]
  
  purrr::map(listaFiles,.f=function(nomeFile){  
    
    regione<-str_remove(str_remove(nomeFile,"\\.serie_valide.csv$"),"^[^.]+\\.")
    
    #lettura dati giornalieri organizzati con lo schema: yy, mm, dd, codice, codice...
    read_delim(nomeFile,delim=";",col_names = TRUE,col_types = YYMMDD_TYPE)->dati
  
    purrr::map_at(dati,.at=4:ncol(dati),listaFunzioni[[ii]]) %>% #ogni funzione in listaFunzioni restituisce TRUE (superamento della soglia) o FALSE
    bind_cols() %>%
    dplyr::select(-dd) %>%
    group_by(yy,mm) %>% #conto tutti i TRUE 
    summarise(across(.fns=sum,na.rm=FALSE)) %>%
    ungroup() ->dfConteggi
    #ho come output un dataframe con yy,mm, codice, codice, codice: ogni colonna dati riporta il numero di superamenti della soglia per quel mese e anno
  

    #per ill calcolo dei climatolotigi giornalieri dobbiamo sapere il numero di dati NA in un mese e il numero di dati validi
    purrr::map_at(dati,.at=3:ncol(dati),.f=~(!is.na(.))) %>%
      bind_cols() %>%
      dplyr::select(-dd) %>%
      group_by(yy,mm) %>%
      summarise(across(.fns=sum,na.rm=FALSE)) %>%
      ungroup()->numeroGiorniPerMese
  
    purrr::map_at(dati,.at=3:ncol(dati),.f=~(is.na(.))) %>%
      bind_cols() %>%
      dplyr::select(-dd) %>%
      group_by(yy,mm) %>%
      summarise(across(.fns=sum,na.rm=FALSE)) %>%
      ungroup()->numeroGiorniMeancantiPerMese


    codiciStazioni<-base::setdiff(names(dfConteggi),c("yy","mm"))
      
    #ciclo sulle stazioni: sto considerando un prediso dataframe (indice) e comincio a iterare sulle stazioni
    purrr::map(codiciStazioni,.f=function(codice){
      
      #df è un tibble in cui metto i conteggi per mese e anno di quella stazione, assieme al numero di giorni presenti e assenti per quel mese e anno
      tibble(yy=dfConteggi[["yy"]],
             mm=dfConteggi[["mm"]],
             indice=dfConteggi[[codice]],
             giorniValidi=numeroGiorniPerMese[[codice]],
             giorniMancanti=numeroGiorniMeancantiPerMese[[codice]])->df
      
      df %>%
        mutate(rapporto=indice/giorniValidi) %>% #definizione wmo
        mutate(rapporto=ifelse(giorniMancanti>MANCANTI_AMMESSI,NA,rapporto)) %>% #se pero' il numero di giorni mancanti è troppo invalido rapporto
        mutate(clim61=ifelse(yy>=1961 & yy<=1990,1,0)) %>%  #creo colonne che mi permettono di ifentificare gli anni di ciascun trentennio
        mutate(clim71=ifelse(yy>=1971 & yy<=2000,1,0)) %>%   
        mutate(clim81=ifelse(yy>=1981 & yy<=2010,1,0)) %>%   
        mutate(clim91=ifelse(yy>=1991 & yy<=2020,1,0))->df
    
      #calcolo il climatologico mensile di rapporto: questo climatologico lo calcolo per i vari trentenni
        
      purrr::map(paste0("clim",seq(61,91,by=10)),.f=function(climatologico){
            
        df[,c("yy","mm","rapporto",climatologico)]->sub_df
        names(sub_df)[4]<-"trentennio"
        
        sub_df %>%
            filter(trentennio==1) %>% #prendo solo gli anni con 1 nel trentennio, cio' prendo solo gli anni che appartengono a un determinato trentennio climatologico
            dplyr::select(yy,mm,rapporto) %>%
            dplyr::select(-yy) %>% 
            group_by(mm) %>% #climatologico mensile
            summarise(climatologico=media(rapporto)) %>% #media
            ungroup()->out
            
        #definizione wmo
        out$climatologico<-out$climatologico*c(31,28.25,31,30,31,30,31,31,30,31,30,31)
        names(out)[2]<-climatologico
        out

      }) %>%reduce(.f=left_join)->dfClimatologici
      
      dfClimatologici$id<-paste0(regione,"_",codice)
    
      return(dfClimatologici)
          
    }) %>% bind_rows()->dfOut
    
    

    dfOut %>% 
      mutate(mm=month.name[mm]) %>% 
      mutate(mm=factor(mm,levels=month.name,ordered = TRUE)) %>%
      gather(key="yy",value="climatologico",-mm,-id) %>% 
      spread(key=mm,value=climatologico) %>%
      mutate(yy=case_when(yy=="clim61"~"1975",
                            yy=="clim71"~"1985",
                            yy=="clim81"~"1995",
                            yy=="clim91"~"2005")) %>%
      mutate(yy=as.integer(yy)) %>%
      dplyr::select(yy,id,everything())->dfRegione
      
    return(dfRegione)
      
      
  }) %>% bind_rows()->daScrivere

  #vogliamo tenere solo le stazioni per cui abbiamo calcolato il climatologico mensile e annuale di precipitazione. Perche'? Perche quando sono stati calcolati
  #i climatologici mensili di precipitazione è stata valutata l'omogeneitò (mediante il test di Pettit) delle serie cumulate annuale. In questo programma
  #invece questo controllo non e' stato ripetuto.
  
  if(PARAM=="prcp"){
    
    read_delim("climatologici_mensili_prec_homog.csv",delim=";",col_names = TRUE) %>%
      dplyr::select(yy,id)->clim_mensili_prec
    
    left_join(clim_mensili_prec,daScrivere)->daScrivere
    
  }
  

 
  nrow(daScrivere)->numeroRighe
  
  purrr::map(1:numeroRighe,.f=function(riga){
    
    apply(map_dfc(daScrivere[riga,] %>% 
      dplyr::select(-yy,-id),.f=~(is.na(.))),1,FUN=sum)->numeroMesiNonValidi
    
    if(numeroMesiNonValidi==12) return()
    apply(daScrivere[riga,] %>%dplyr::select(-yy,-id),1,FUN=sum,na.rm=FALSE)->annuale
    
    out<-daScrivere[riga,]
    out$annuale<-annuale
    
    out
    
  }) %>% compact() %>% bind_rows()->daScrivere2
  

  if(!nrow(daScrivere2)) return()

  write_delim(daScrivere2 %>% dplyr::select(-annuale),glue::glue("climatologici_mensili_{qualeIndice}_homog.csv"),delim=";",col_names = TRUE)

  daScrivere2 %>%
    dplyr::select(yy,id,annuale) %>%
    filter(!is.na(annuale)) %>%
    spread(key=yy,value=annuale) %>%
    write_delim(.,glue::glue("climatologici_annuali_{qualeIndice}_homog.csv"),delim=";",col_names = TRUE)

})