package tbls

import (
	"github.com/llmofang/kdbgo"
)

type Ohlcv struct {
	Sym    string
	Minute kdb.Minute
	Open   int32
	High   int32
	Low    int32
	Close  int32
	Size   int32
}
