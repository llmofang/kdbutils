package tbls

import (
	"time"
	"code.google.com/p/go-uuid/uuid"
	"strings"
)

/**
kdb table define:
Position:3!flip `sym`accountname`stockcode`position`price`marketprice`profit!"sssifff"$\:()
Profit:3!flip `sym`accountname`stockcode`profit`entrusts`volumes`amount`fee!"sssfiiff"$\:()
 */

const(
	STATUS_UNREPORTED int32=iota
	STATUS_REPORTED
	STATUS_NOTTRADED
	STATUS_NOTCANCELED
	STATUS_TRADED
	STATUS_CANCELED
	STATUS_BADORDER
)




type Request struct {
	Sym         string
	Qid         string
	Accountname string
	Time        time.Time
	Entrustno   int32
	Stockcode   string
	Askprice    float64
	Askvol      int32
	Bidprice    float64
	Bidvol      int32
	Withdraw    int32
	Status      int32
}

type Request64 struct {
	Sym         string
	Qid         string
	Accountname string
	Time        time.Time
	Entrustno   int64
	Stockcode   string
	Askprice    float64
	Askvol      int64
	Bidprice    float64
	Bidvol      int64
	Withdraw    int64
	Status      int64
}

type Response Request
type Response64 Request64

type Entrust Request

type RequestXX Request

type Entrust64 Request64

type Position struct {
	Sym         string
	Accountname string
	Stockcode   string
	Position    int32
	Price       float64
	Marketprice float64
	Profit      float64
}

type Profit struct {
	Sym         string
	Accountname string
	Stockcode   string
	Profit      float64
	Entrusts    int32
	Volumes     int32
	Amount      float64
	Fee	    float64
}

// autocloserequest:1!flip `sym`time`username`accountname`stockcode`volume`status`note!"szsssiis"$\:();
type AutoCloseRequst struct {
	Sym string
	Time time.Time
	Username string
	Accountname string
	Stockcode string
	Volume int32
	Status int32
	Note string
}


func NewEntrust(sym string,accountname string,stockcode string,price float64,vol int32)*Entrust {
	entrust := &Entrust{}
	entrust.Sym = sym
	entrust.Accountname = accountname
	entrust.Askprice = price
	entrust.Askvol = vol
	entrust.Qid ="close"+strings.Replace(uuid.New(),"-","",-1)
	entrust.Status = 0
	entrust.Time = time.Now()
	entrust.Stockcode = stockcode
	return entrust

}



type AutoCloseRequest struct {
	Sym string
	Time time.Time
	Username string
	Accountname string
	Stockcode string
	Volume int32
	Status string
	Note string
}



type AccountQuota struct {
	Sym                string
	StockCode          string
	AmountBuy          int32
	AmountSell         int32
	AmountBuyReserved  int32
	AmountSellReserved int32
	Priority           int32
}
