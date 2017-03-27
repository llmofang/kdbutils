package connectionpool

import (
	"errors"
	"sync"
	"fmt"
	"time"
)

type ConnectionPool struct{
	host string
	port int
	connections []*PooledKDB
	initialConnNums int
	maxConnNums int
	incrementalNums int
	connIndex int
	isMax bool
	sync.RWMutex


}

func NewConnectionPool(host string,port int)*ConnectionPool{
	connectionPool:=ConnectionPool{}
	connectionPool.host=host
	connectionPool.port=port
	connectionPool.connections=nil
	connectionPool.initialConnNums=50
	// maxConnNums <0 表示没有最大连接数量
	connectionPool.maxConnNums=50
	connectionPool.incrementalNums=5
	connectionPool.connIndex=0
	connectionPool.isMax=false
	return &connectionPool
}

func (this *ConnectionPool)SetMaxConnNums(num int){
	this.maxConnNums=num
}

func(this *ConnectionPool)SetIncrementalNums(num int){
	this.incrementalNums=num
}


func(this *ConnectionPool)SetInitialConnNums(num int){
	this.initialConnNums=num
}

func(this *ConnectionPool)NextIndex()int{
	this.connIndex++
	return this.connIndex
}


func (this *ConnectionPool)CreatePool(){
	if this.connections!=nil{
		return
	}
	this.connections=[]*PooledKDB{}
	this.CreateConnections(this.initialConnNums)


}


func(this *ConnectionPool)CreateConnections(num int){
	this.Lock()
	defer this.Unlock()
	if this.isMax{
		return
	}
	for i:=0;i<num;i++{
		if(this.maxConnNums>0&&len(this.connections)>=this.maxConnNums){
			fmt.Println("connections is reach max connection numbers",len(this.connections),this.maxConnNums)
			this.isMax=true
			return
		}
		conn:=this.newConnection()
		this.connections=append(this.connections,conn)

	}
}


func(this *ConnectionPool)newConnection()*PooledKDB{

	pooledKDB:=NewPooledKDB(this.host,this.port,this.NextIndex())
	pooledKDB.Connect()
	if len(this.connections)==0{
		//第一次创建数据库连接的时候可做一些初始化操作
	}
	return pooledKDB
}

func(this *ConnectionPool)GetConnection()*PooledKDB{
	for{
		connection:=this.findFreeConnection()
		if connection!=nil{
			return connection
		}
		if !this.isMax{
			this.CreateConnections(this.incrementalNums)
		}
		time.Sleep(10*time.Millisecond)
	}

}

func(this *ConnectionPool)findFreeConnection()*PooledKDB{
	//this.Lock()
	//defer this.Unlock()
	for _,connection:=range this.connections{
		if(!connection.isBusy){
			//this.Lock()
			connection.isBusy=true
			//this.Unlock()
			// TODO 测试连接是否可用
			return connection
		}
	}
	//for _,t:=range this.connections{
	//	fmt.Println("no free connection",t.id,t.isBusy)
	//}

	return nil
}


func(this *ConnectionPool)ReturnConnection(connection *PooledKDB){
	//this.Lock()
	//defer this.Unlock()
	if(this.connections==nil){
		errors.New("连接池不存在！！！")
	}
	for _,conn:=range this.connections{
		if conn.id==connection.id{
			//this.Lock()
			conn.isBusy=false
			//this.Unlock()
			break
		}
	}
	//this.Lock()
	//connection.isBusy=false
	//this.Unlock()

}


func(this *ConnectionPool)CloseConnectionPool()error{
	if this.connections==nil{
		errors.New("连接池不存在！！！")
	}
	for _,connection:=range this.connections{
		err:=connection.Close()
		if err!=nil{
			return err
		}
	}
	return nil
}
