package tbls

import (
	"time"
	"unsafe"
)

type AutoCloseSignal struct {
	Sym        string
	Time       time.Time
	Name       string
	Signaltype string
	Signal     float64
	Strength   float64
	Note       string
	BuyInSpread int32
	SellInSpread int32
	BuyMO int32
	SellMO int32
	AveStdX2dRet float64
	PredictPrice int32
	Tick *Market
}


func (this *AutoCloseSignal) Size() int {
	temp := (int)(unsafe.Sizeof(this))
	return temp
}
