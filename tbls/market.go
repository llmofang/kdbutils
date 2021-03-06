package tbls

import (
	"time"
	"unsafe"
)

type Market struct {
	Sym string
	Time time.Duration
	SzWindCode string
	NActionDay int32
	NTime int32
	NStatus int32
	NPreClose int32
	NOpen int32
	NHigh int32
	NLow int32
	NMatch int32
	NAskPrice1 int32
	NAskPrice2 int32
	NAskPrice3 int32
	NAskPrice4 int32
	NAskPrice5 int32
	NAskPrice6 int32
	NAskPrice7 int32
	NAskPrice8 int32
	NAskPrice9 int32
	NAskPrice10 int32
	NAskVol1 int32
	NAskVol2 int32
	NAskVol3 int32
	NAskVol4 int32
	NAskVol5 int32
	NAskVol6 int32
	NAskVol7 int32
	NAskVol8 int32
	NAskVol9 int32
	NAskVol10 int32
	NBidPrice1 int32
	NBidPrice2 int32
	NBidPrice3 int32
	NBidPrice4 int32
	NBidPrice5 int32
	NBidPrice6 int32
	NBidPrice7 int32
	NBidPrice8 int32
	NBidPrice9 int32
	NBidPrice10 int32
	NBidVol1 int32
	NBidVol2 int32
	NBidVol3 int32
	NBidVol4 int32
	NBidVol5 int32
	NBidVol6 int32
	NBidVol7 int32
	NBidVol8 int32
	NBidVol9 int32
	NBidVol10 int32
	NNumTrades int32
	IVolume int64
	ITurnover int64
	NTotalBidVol int32
	NTotalAskVol int32
	NWeightedAvgBidPrice int32
	NWeightedAvgAskPrice int32
	NIOPV int32
	NYieldToMaturity int32
	NHighLimited int32
	NLowLimited int32
	NSyl1 int32
	NSyl2 int32
	NSD2 int32
}


func (this *Market) Size() int {
	temp := (int)(unsafe.Sizeof(this))
	return temp
}



type Kline struct{
	Time time.Duration
	Sym string
	NDate int32
	NTime int32
	Open int64
	High int64
	Low int64
	Close int64
	NumTrades int64
	TotalVolume int64
	TotalValue int64
}
