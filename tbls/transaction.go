package tbls

import "time"

type Transaction struct {
	Time   time.Time
	Sym string
	SzWindCode   string
	NActionDay   int32
	NTime    int32
	NIndex  int32
	NPrice   int32
	NVolume int32
	NTurnover int32
	NBSFlag string
	ChOrderKind string
	ChFunctionCode string
	NAskOrder int32
	NBidOrder int32
}