package kdbutils

import (
	"github.com/llmofang/kdbutils/tbls"
	"fmt"
)

type TradeKDB struct {
	*Kdb
	TableStruct map[string]Factory_New
}

func NewTradeKDB(host string, port int)*TradeKDB{
	tradeKDB:=TradeKDB{NewKdb(host,port),make(map[string]Factory_New)}
	tradeKDB.TableStruct["response"]=func() interface{} {
		return new(tbls.Entrust)
	}
	tradeKDB.TableStruct["request"]=func() interface{} {
		return new(tbls.Entrust)
	}
	tradeKDB.TableStruct["Position"]=func() interface{} {
		return new(tbls.Position)
	}
	return &tradeKDB
}

func (this * TradeKDB)Trade(entrust *tbls.Entrust){

	this.FuncTable("upd","request",[]tbls.Entrust{*entrust})
}


func(this *TradeKDB)SelectEntrustWithQid(qid string)*tbls.Entrust{
	entrusts := []tbls.Entrust{}
	ret,err:=this.QueryNoneKeyedTable("0! select [-1] from response where qid=`"+qid,&entrusts)
	if err!=nil{
		fmt.Println(err)
	}
	if ret==nil{
		return nil
	}
	entrusts=ret.([]tbls.Entrust)

	if len(entrusts)==0{
		return nil
	}
	return &entrusts[0]
}


func(this *TradeKDB)Cancel(entrust *tbls.Entrust){
	entrust.Status=3
	this.FuncTable("upd","request",[]tbls.Entrust{*entrust})
}
