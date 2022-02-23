rm(list=objects())
library("data.table")
library("vroom")
library("dplyr")
library("guido")
library("PCICt")
library("climdex.pcic")
library("config")
library("stringr")
library("glue")
library("ggplot2")

basename(getwd())->area

purrr::partial(.f=config::get,file ="../estremi_prec.yml")->myget


vroom("prpc_nord_serie_selezionate_per_estremi_rapporto_1991_2020.csv",delim=";",col_names = TRUE,col_types = YYMMDD_TYPE ) %>%
  setDT()->tmp

tmp[,yymmdd:=.(as.PCICt(paste0(yy,"-",mm,"-",dd),cal = "gregorian") )][(yy>=myget(value="annoI")) & (yy<=myget(value="annoF"))]->dati


purrr::map(names(dati)[!names(dati) %in% c("yy","mm","dd","yymmdd")],.f=~(climdexInput.raw(prec = dati[[.]],
                                  prec.dates = dati$yymmdd,
                                  base.range = c(myget(value="climatolI"),
                                                 myget(value="climatolF")),
                                  max.missing.days = c(annual=myget(value="max.missing.days.annual") ,monthly=myget(value="max.missing.days.monthly")))))->inputPerIndici

names(inputPerIndici)<-names(dati)[!names(dati) %in% c("yy","mm","dd","yymmdd")]

funzioni<-list("climdex.r95ptot","climdex.sdii","climdex.r10mm")

purrr::walk(funzioni,.f=function(myfun){
  
  str_remove(myfun,"climdex\\.")->nomeIndice
  
  purrr::map_dfc(inputPerIndici,.f=base::get(myfun))->df
  
  purrr::map_dfc(df,.f=function(.x){
    
    .x[names(.x) %in% as.numeric(myget(value="climatolI")):as.numeric(myget(value="climatolF"))]->dati_per_climatologico
    dati_per_climatologico[!is.na(dati_per_climatologico)]->dati_per_climatologico
    ifelse(length(dati_per_climatologico)<24,NA,mean(dati_per_climatologico,na.rm = TRUE))->media
    tibble(anomalia=.x-media)
    
    
  })->dfAnomalie
  
  
  apply(dfAnomalie,MARGIN = 1,FUN = mean,na.rm=TRUE)->anomaliaItalia
  
  df$yy<-seq(myget(value='annoI'),myget(value='annoF'))
  df$yy->dfAnomalie$yy

  
  names(dfAnomalie)<-names(df)
  vroom::vroom_write(df %>% dplyr::select(yy,everything()),glue::glue("{nomeIndice}_{area}_{myget(value='annoI')}_{myget(value='annoF')}.csv"),delim=";",col_names = TRUE)
  vroom::vroom_write(dfAnomalie %>% dplyr::select(yy,everything()),glue::glue("anom_{nomeIndice}_{area}_{myget(value='annoI')}_{myget(value='annoF')}.csv"),delim=";",col_names = TRUE)
  
  tibble(yy=df$yy,anom=anomaliaItalia)->dfAnomItalia
  vroom::vroom_write(dfAnomItalia,glue::glue("anomItalia_{nomeIndice}_{area}_{myget(value='annoI')}_{myget(value='annoF')}.csv"),delim=";",col_names = TRUE)
  
  pdf(glue::glue("{nomeIndice}_{area}.pdf"),width = 10,height=8)
  ggplot(data=dfAnomItalia %>% mutate(segno=ifelse(anom>=0,"red","blue")))+
    geom_bar(stat="identity",aes(x=yy,y=anom,fill=segno))+
    theme_bw()->grafico
  print(grafico)
  dev.off()
  
  
})

