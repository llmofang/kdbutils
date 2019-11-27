package tbls

import (
	"time"
)
type Order struct {
	Sym string
	Time time.Duration
	SzWindCode string
	NActionDay int32
	NTime int32
	NOrder int32
	NPrice int32
	NVolume int32
	ChOrderKind byte
	ChFunctionCode byte
}

type Index struct{
	Time time.Duration
	Sym string
	SzWindCode string
	NActionDay int32
	NTime int32
	NOpenIndex int32
	NHighIndex int32
	NLowIndex int32
	NLastIndex int32
	ITotalVolume int64
	ITurnover int64
	NPreCLoseIndex int32
}