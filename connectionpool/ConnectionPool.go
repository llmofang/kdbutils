package connectionpool

import (
	"sync"
	"fmt"
)

type ConnectionPool struct{
	host string
	port int
	connections *Stack
	initialConnNums int
	maxConnNums int
	incrementalNums int
	connIndex int
	connNums int
	sync.RWMutex


}

func NewConnectionPool(host string,port int)*ConnectionPool{
	connectionPool:=ConnectionPool{}
	connectionPool.host=host
	connectionPool.port=port
	connectionPool.connections=NewStack()
	connectionPool.initialConnNums=100
	// maxConnNums <0 表示没有最大连接数量
	connectionPool.maxConnNums=200
	connectionPool.incrementalNums=5
	connectionPool.connIndex=0
	connectionPool.connNums=0
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
	for i:=0;i<num;i++{
		if(this.maxConnNums>0&&this.connNums>=this.maxConnNums){
			fmt.Println("connections is reach max connection numbers",this.connections.Len(),this.maxConnNums)
			return
		}
		conn:=this.newConnection()
		this.connections.Push(conn)
		this.connNums++
	}
}


func(this *ConnectionPool)newConnection()*PooledKDB{
	id:=this.NextIndex()
	fmt.Println("this connection id ",id)
	pooledKDB:=NewPooledKDB(this.host,this.port,id)
	pooledKDB.Connect()
	//if len(this.connections)==0{
	//	//第一次创建数据库连接的时候可做一些初始化操作
	//}
	return pooledKDB
}

func(this *ConnectionPool)GetConnection()*PooledKDB{
	connection:=this.findFreeConnection()
	if connection==nil{
		this.CreateConnections(this.incrementalNums)
	}else{
		return connection
	}
	connection=this.findFreeConnection()
	return connection

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
