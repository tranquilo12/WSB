#Using cryptodatadownload.com as a source

install.packages("Metrics")
library(Metrics)

#So I don't get a peer certificate error
library(httr)
set_config(config(ssl_verifypeer = 0L))

#We'll look at only the following for the time being:
#BTC, BCH, ETH, ETC, EOS, LTC, XRP, XLM
currs<-c("btc","eth","bch","etc","xrp","xlm","eos","ltc")
freqs<-c("d","h") #maybe minutes could be looked at later on

#Using data from cryptodatadownload.com
#Unfortunately EOS/XLM only seem to have pairs against ETC. Might need to multiply with BTC USD afterwards...
#Hourly
btc.h<-read.delim("http://www.cryptodatadownload.com/cdd/gemini_BTCUSD_1hr.csv",header=TRUE,skip=1,sep=",")
#bch.h<-read.delim("http://www.cryptodatadownload.com/cdd/Poloniex_BCHUSD_1hr.csv",header=TRUE,skip=1,sep=",")
eth.h<-read.delim("http://www.cryptodatadownload.com/cdd/gemini_ETHUSD_1hr.csv",header=TRUE,skip=1,sep=",")
etc.h<-read.delim("http://www.cryptodatadownload.com/cdd/Poloniex_ETCUSD_1h.csv",header=TRUE,skip=1,sep=",")
eos.h<-read.delim("http://www.cryptodatadownload.com/cdd/Tidex_EOSBTC_1h.csv",header=TRUE,skip=1,sep=",")
ltc.h<-read.delim("http://www.cryptodatadownload.com/cdd/gemini_LTCUSD_1hr.csv",header=TRUE,skip=1,sep=",")
xrp.h<-read.delim("http://www.cryptodatadownload.com/cdd/Kraken_XRPUSD_1h.csv",header=TRUE,skip=1,sep=",")
xlm.h<-read.delim("http://www.cryptodatadownload.com/cdd/Bittrex_XLMBTC_1h.csv",header=TRUE,skip=1,sep=",")

#Daily
btc.d<-read.delim("http://www.cryptodatadownload.com/cdd/Gemini_BTCUSD_d.csv",header=TRUE,skip=1,sep=",")
#bch.d<-read.delim("http://www.cryptodatadownload.com/cdd/Poloniex_BCHUSD_d.csv",header=TRUE,skip=1,sep=",")
eth.d<-read.delim("http://www.cryptodatadownload.com/cdd/Gemini_ETHUSD_d.csv",header=TRUE,skip=1,sep=",")
etc.d<-read.delim("http://www.cryptodatadownload.com/cdd/Poloniex_ETCUSD_d.csv",header=TRUE,skip=1,sep=",")
eos.d<-read.delim("http://www.cryptodatadownload.com/cdd/Tidex_EOSBTC_d.csv",header=TRUE,skip=1,sep=",")
ltc.d<-read.delim("http://www.cryptodatadownload.com/cdd/Gemini_LTCUSD_d.csv",header=TRUE,skip=1,sep=",")
xrp.d<-read.delim("http://www.cryptodatadownload.com/cdd/Kraken_XRPUSD_d.csv",header=TRUE,skip=1,sep=",")
xlm.d<-read.delim("http://www.cryptodatadownload.com/cdd/Bittrex_XLMBTC_d.csv",header=TRUE,skip=1,sep=",")

#maybe could be used as an rmse replacement?
medae<-function(x,y)return(median(abs(x-y)))

#Defaults given in terms of days... but this could be used for hours too
caterp<-function(price,dur.past=14,dur.pred=28,n.similar=5){
  #price is usually given latest first
  price<-rev(price)
  price.chg<-price[2:length(price)]/price[2:length(price) - 1]
  price.tail<-tail(price.chg,dur.past)
  price.err<-rep(0,length(price.chg)-dur.past-dur.pred)
  for(i in 1:length(price.err)){
    price.err[i]<-rmse(price.tail,price.chg[1:dur.past + i - 1])
  }
  tops<-which(price.err %in% head(sort(price.err,decreasing=FALSE),n.similar))
  toperrs<-price.err[price.err %in% head(sort(price.err,decreasing=FALSE),n.similar)]
  weighted.pred<-rep(0,dur.pred)
  for(i in 1:n.similar){
    weighted.pred<-weighted.pred+price.chg[(dur.past+tops[i]):(dur.past+dur.pred+tops[i]-1)]*((1/toperrs[i])/sum(1/toperrs))
  }
  price.pred<-rep(0,dur.pred)
  price.pred[1]<-tail(price,1)*weighted.pred[1]
  for(i in 2:dur.pred){
    price.pred[i]<-price.pred[i-1]*weighted.pred[i]
  }
  return(price.pred)
}

#Can try this out:
caterp(btc.h$Close,168,168,3)
caterp(eth.h$Close,168,168,3)
