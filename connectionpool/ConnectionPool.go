package connectionpool

import (
	"sync"
	"fmt"
	"time"
)
/**
/默认初始连接10,，每次增加5，最大连接200

 */

type ConnectionPool struct{
	host string
	port int
	connections *Stack
	initialConnNums int
	maxConnNums int
	incrementalNums int
	connIndex int
	connNums int
	isMax bool
	sync.RWMutex


}

func NewConnectionPool(host string,port int)*ConnectionPool{
	connectionPool:=ConnectionPool{}
	connectionPool.host=host
	connectionPool.port=port
	connectionPool.connections=NewStack()
	connectionPool.initialConnNums=10
	// maxConnNums <0 表示没有最大连接数量
	connectionPool.maxConnNums=200
	connectionPool.incrementalNums=5
	connectionPool.connIndex=0
	connectionPool.connNums=0
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
	//if this.connections.Len()!=0{
	//	return
	//}
	//this.connections=NewStack()
	this.CreateConnections(this.initialConnNums)


}


func(this *ConnectionPool)CreateConnections(num int){

	this.Lock()
	defer this.Unlock()
	if this.isMax==true{
		return
	}
	for i:=0;i<num;i++{
		if(this.maxConnNums>0&&this.connNums>=this.maxConnNums){
			fmt.Println("connections is reach max connection numbers",this.connections.Len(),this.maxConnNums)
			this.isMax=true
			return
		}
		conn:=this.newConnection()
		this.connections.Push(conn)
		this.connNums++
	}
}


func(this *ConnectionPool)newConnection()*PooledKDB{
	id:=this.NextIndex()
	pooledKDB:=NewPooledKDB(this.host,this.port,id)
	pooledKDB.Connect()
	//if len(this.connections)==0{
	//	//第一次创建数据库连接的时候可做一些初始化操作
	//}
	return pooledKDB
}

func(this *ConnectionPool)GetConnection()*PooledKDB{
	for {

		connection:=this.findFreeConnection()
		if connection!=nil{
			return connection
		}
		if connection==nil&&!this.isMax{
			this.CreateConnections(this.incrementalNums)
		}else{
			time.Sleep(100*time.Millisecond)
		}

	}


}

func(this *ConnectionPool)findFreeConnection()*PooledKDB{

	//for _,connection:=range this.connections{
	//	if(!connection.isBusy){
	//		this.Lock()
	//		connection.isBusy=true
	//		this.Unlock()
	//		// TODO 测试连接是否可用
	//		return connection
	//	}
	//}
	item:=this.connections.Pop()
	if(item==nil){
		return nil
	}

	pooledKDB:=item.(*PooledKDB)

	return pooledKDB
}


func(this *ConnectionPool)ReturnConnection(connection *PooledKDB){

	//if(this.connections==nil){
	//	errors.New("连接池不存在！！！")
	//}
	this.connections.Push(connection)


}


func(this *ConnectionPool)CloseConnectionPool()error{
	//if this.connections==nil{
	//	errors.New("连接池不存在！！！")
	//}
	for(this.connections.Len()!=0){
		item:=this.connections.Pop()
		conn:=item.(*PooledKDB)
		conn.Close()
	}
	return nil
}
