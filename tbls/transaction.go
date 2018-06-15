package tbls

import "time"

type Transaction struct {
	Time   time.Duration
	Sym string
	SzWindCode   string
	NActionDay   int32
	NTime    int32
	NIndex  int32
	NPrice   int32
	NVolume int32
	NTurnover int32
	NBSFlag int32
	ChOrderKind byte
	ChFunctionCode byte
	NAskOrder int32
	NBidOrder int32
}

