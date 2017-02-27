package connectionpool

import "github.com/llmofang/kdbutils"

type PooledKDB struct {
	isBusy bool
	kdbutils.Kdb
	id int

}

func NewPooledKDB(host string,port int,index int)*PooledKDB{
	pooledKDB:=PooledKDB{}
	pooledKDB.Host=host
	pooledKDB.Port=port
	pooledKDB.isBusy=false
	pooledKDB.id=index
	return &pooledKDB
}





