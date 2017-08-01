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
	tradeKDB.TableStruct["response1"]=func() interface{} {
		return new(tbls.Entrust)
	}
	tradeKDB.TableStruct["request"]=func() interface{} {
		return new(tbls.Entrust)
	}

	tradeKDB.TableStruct["Position1"]=func() interface{} {
		return new(tbls.Position)
	}


	tradeKDB.TableStruct["AccountQuotaUpdate"]=func() interface{}{
		return new(tbls.AccountStockQuota)
	}
	tradeKDB.TableStruct["UserQuotaUpdate"]=func()interface{}{
		return new(tbls.UserStockQuota)
	}

	return &tradeKDB
}

func (this * TradeKDB)Trade(entrust *tbls.Entrust){

	this.AsyncFuncTable("upd","requestxx",[]tbls.Entrust{*entrust})
}


func(this *TradeKDB)SelectEntrustWithQid(qid string)*tbls.Entrust{
	entrusts := []tbls.Entrust{}
	ret,err:=this.QueryNoneKeyedTable("0! select [-1] from response where qid=`"+qid,&entrusts)
	if err!=nil{
		fmt.Println(err)
	}
	entrusts=ret.([]tbls.Entrust)
	if len(entrusts)==0{
		return nil
	}

	return &entrusts[0]
}


func(this *TradeKDB)Cancel(entrust *tbls.Entrust){
	entrust.Status=3
	this.AsyncFuncTable("upd","requestxx",[]tbls.Entrust{*entrust})
}


func(this *TradeKDB)GetPositions(code string,users []string)[]*tbls.Position{
	positions:=[]tbls.Position{}
	ret,err:=this.QueryNoneKeyedTable("0!select from Position where stockcode=`"+code,&positions)
	if err!=nil{
		fmt.Println(err)
	}
	positions=ret.([]tbls.Position)
	positionsWithUsers:=[]*tbls.Position{}
	if users!=nil{
		for _,position:=range positions{
			for _,user:=range users{
				if position.Sym==user{
					positionsWithUsers=append(positionsWithUsers,&position)
				}
			}
		}
		return positionsWithUsers
	}
	return positionsWithUsers
}