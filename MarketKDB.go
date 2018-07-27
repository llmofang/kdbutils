package kdbutils

import (
	"github.com/llmofang/kdbutils/tbls"
	"fmt"
)

type MarketKDB struct {
	*Kdb
	TableStruct  map[string]Factory_New
}

func NewMarketKDB(host string, port int)*MarketKDB{
	this:=MarketKDB{NewKdb(host,port),make(map[string]Factory_New)}
	this.TableStruct["Market"]=func() interface{} {
		return new(tbls.Market)
	}
	this.TableStruct["Transaction"]=func() interface{}{
		return new(tbls.Transaction)
	}

	this.TableStruct["Kline"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline1m"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline5m"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline15m"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline30m"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline60m"]= func() interface{} {
		return new(tbls.Kline)
	}
	this.TableStruct["Kline1d"]= func() interface{} {
		return new(tbls.Kline)
	}

	return &this
}

func (this *MarketKDB)GetLastTickData(stockcode string)*tbls.Market {
	markets := []tbls.Market{}
	ret,err:=this.QueryNoneKeyedTable("0! select [-1] from Market where sym=`"+stockcode,&markets)
	markets=ret.([]tbls.Market)
	if err!=nil{
		fmt.Println(err)
	}
	if len(markets)==0{
		return nil
	}


	return &markets[0]

}



