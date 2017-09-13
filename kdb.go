package kdbutils

import (
	logger "github.com/alecthomas/log4go"
	"fmt"
	"errors"
	"reflect"
	"strings"
	"time"
	"sync"
	"github.com/llmofang/kdbutils/comm"
	"github.com/llmofang/kdbgo"
)

// ref: http://stackoverflow.com/questions/10210188/instance-new-type-golang

type Factory_New func() interface{}

type Kdb struct {
	Host       string
	Port       int
	Connection *kdb.KDBConn
	subscriber comm.Subscriber
	sub_tables []string
	OutputChan chan interface{}
	InputChan  chan comm.FuncTable
	channelClosed bool
	closed bool
	sync.RWMutex


}

func NewKdb(host string, port int) *Kdb {
	kdb := &Kdb{Host:host, Port: port, Connection:nil,
		subscriber: comm.Subscriber{Set:make(map[string]int, 0)},
		sub_tables:make([]string, 0), OutputChan:make(chan interface{}, 10000), InputChan:make(chan comm.FuncTable, 10000)}


	return kdb
}


func(this *Kdb)Close()error {

	return this.Connection.Close()
}

func(this *Kdb)CloseOutputChan(){
	this.channelClosed=true
	close(this.OutputChan)
}



func (this *Kdb) Start(table2struct map[string]Factory_New) {
	if !this.IsConnected() {
		this.Connect()
	}

	go this.GetCommandFromChannel()
	go this.SubscribedData2Channel(table2struct)
}

func (this *Kdb) DumpSubscriber() {
	this.subscriber.Dump()
}

func (this *Kdb) GetCommandFromChannel() {
	var func_table comm.FuncTable
	Test := false
	for {
		if this.closed{
			return
		}
		func_table = <-this.InputChan
		logger.Debug("Channel Market Length: %v", len(this.InputChan))
		logger.Debug("Get new command from channel, FuncTable:", func_table)
		switch func_table.FuncName {
		case "":
			logger.Error("FuncTable's FuncName is empty, func_table: %v", func_table)
		case comm.SubFunc:
			if func_table.TableName == "" {
				sym := func_table.Data.([]string)
				this.SubSym(sym)
			} else {
				sym := func_table.Data.([]string)
				this.Subscribe(func_table.TableName, sym)
			}
		case comm.UnSubFunc:
			if func_table.TableName == "" {
				sym := func_table.Data.([]string)
				this.UnSubSym(sym)
			}
		default:
			if !Test {
				logger.Debug("FuncTable ......:", func_table)
				this.AsyncFuncTable(func_table.FuncName, func_table.TableName, func_table.Data)
			}
		}
	}

}

func (this *Kdb) Connect() error {
	logger.Info("connecting to kdb, host: %v, port:%v", this.Host, this.Port)
	var err error
	if this.Connection, err = kdb.DialKDB(this.Host, this.Port, "kdbgo:kdbgo"); err == nil {
		logger.Info("Connected to kdb successful")
	} else {
		logger.Error("Failed to connect to kdb, error: %s", err.Error())
		return err
	}
	return nil
}

func (this *Kdb) IsConnected() bool {
	if this.Connection != nil {
		return true
	} else {
		return false
	}
}

func (this *Kdb) Disconnect() error {
	logger.Info("disconnecting to kdb, host: %v, port:%v", this.Host, this.Port)
	this.closed=true
	err := this.Connection.Close()
	if err == nil {
		logger.Info("disconnected to kdb successful")
	} else {
		logger.Error("disconnected to kdb error, err: %s", err.Error())
	}
	return err
}

func (this *Kdb) Subscribe(table string, sym []string) {
	sym_num := len(sym)
	logger.Debug("Subscribing Kdb, table: %s, sym: %v, sym_num: %v", table, sym, sym_num)
	var err error

	this.Lock()
	if sym_num == 0 {
		err = this.Connection.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{-kdb.KS, kdb.NONE, ""})
	} else {
		err = this.Connection.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{kdb.KS, kdb.NONE, sym})
	}
	this.Unlock()

	if err != nil {
		logger.Error("Failed to subscibe, table: %s, sym; %s", table, sym)
	}
}

func (this *Kdb) SubSym(sym []string) {

	logger.Debug("SubSym parameters, table: %s, sym: %v", this.sub_tables, sym)

	if len(this.sub_tables) == 0 || len(sym) == 0 {
		logger.Debug("subtables or sym length is 0, sub_table: %v, sym :%v", this.sub_tables, sym)
		return
	}

	var sub_syms []string

	for _, symbol := range sym {
		if rtn := this.subscriber.Subscribe(symbol); rtn != nil {
			sub_syms = rtn
		}
	}

	if sub_syms != nil {
		for _, table := range this.sub_tables {
			this.Subscribe(table, sub_syms)
		}
	}
}

func (this *Kdb) UnSubSym(sym []string) {

	logger.Debug("UnSubSym parameters, table: %s, sym: %v", this.sub_tables, sym)

	if len(this.sub_tables) == 0 || len(sym) == 0 {
		logger.Debug("subtables or sym length is 0, sub_table: %v, sym :%v", this.sub_tables, sym)
		return
	}

	var sub_syms []string

	for _, symbol := range sym {
		if rtn := this.subscriber.Unsubscribe(symbol); rtn != nil {
			// logger.Debug("rtn: %v", rtn)
			sub_syms = rtn
		} else {
			// logger.Debug("to_slice", this.subscriber.ToSlice())
		}
	}

	if sub_syms != nil {
		for _, table := range this.sub_tables {
			this.Subscribe(table, sub_syms)
		}
	}
}

func (this *Kdb) SubTable(table string) {

	found := false
	for _, old_tab := range this.sub_tables {
		if old_tab == table {
			logger.Debug("Table already in sub_tables, table: %v", table)
			break
		}
	}
	if !found {
		this.sub_tables = append(this.sub_tables, table)
		sym := this.subscriber.ToSlice()

		if len(sym) > 0 {
			this.Subscribe(table, sym)
		} else {
			logger.Debug("Sym is empty!")
		}
	}
}

func (this *Kdb) SubscribedData2Channel(table2struct map[string]Factory_New) {
	for {
		if this.channelClosed||this.closed{
			return
		}


		// ignore type print output
		res, _, err := this.Connection.ReadMessage()
		if err != nil {
			logger.Error("Error on processing KDB message, error: ", err.Error())
			return
		}
		len := res.Len()
		if len != 3 {
			s := res.Data.(string)

			if s=="\"heartbeat\""{
				continue
			}
			logger.Error("Message is not pub data, length: %v data:%v", len,res)
			continue
		}

		data_list := res.Data.([]*kdb.K)

		// confirm function is upd
		func_name := data_list[0].Data.(string)
		if func_name != "upd" {
			logger.Error("function name is not upd, func_name: %s", func_name)
			continue
		}

		table_name := data_list[1].Data.(string)
		//logger.Debug("message's table_name: %s", table_name)
		var factory Factory_New
		match := false
		for tab, fn := range table2struct {
			if table_name == tab {
				factory = fn
				match = true
				break
			}
		}
		if !match {
			logger.Error("table name not match")
			continue
		}

		var table_data kdb.Table
		switch data_list[2].Data.(type) {
		case kdb.Table:
			table_data = data_list[2].Data.(kdb.Table)
		case kdb.Dict:
			dic := data_list[2].Data.(kdb.Dict)
			logger.Error("received not a table , is a dic, dic: %v", dic)
			continue
		}
		length := table_data.Data[0].Len()
		//logger.Finest("message's table_name: %s, length: %d", table_name, length)
		for i := 0; i < length; i++ {
			row := factory()
			test := table_data.Index(i)
			err := kdb.UnmarshalDict(test, row)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			//logger.Finest("before send: %v", row)
			this.OutputChan <- row
		}
	}
}

func (this *Kdb) QueryNoneKeyedTable(query string, v interface{}) (interface{}, error) {
	if this.Connection == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	this.Lock()
	res, err := this.Connection.Call(query);
	this.Unlock()

	if err != nil {
		logger.Error("Kdb query error, query: %v, error: %v", query, err)
		return nil, errors.New("kdb query error")
	} else {
		// parse result
		t := res.Data.(kdb.Table)
		vv := reflect.ValueOf(v)
		if vv.Kind() != reflect.Ptr || vv.IsNil() {
			return nil, errors.New("Invalid target type. Shoult be non null pointer")
		}
		vv = reflect.Indirect(vv)
		for i := 0; i < int(t.Data[0].Len()); i++ {
			emptyelem := reflect.New(vv.Type().Elem())
			err := kdb.UnmarshalDict(t.Index(i), emptyelem.Interface())
			if err != nil {
				fmt.Println("Failed to unmrshall dict", err)
				return nil, err
			}
			vv = reflect.Append(vv, reflect.Indirect(emptyelem))
		}
		return vv.Interface(), nil
	}
}

func (this *Kdb) AsyncCall(table string, query string) (error) {
	if this.Connection == nil {
		logger.Error("Kdb is not connected")
		return errors.New("kdb is not connected")
	}

	// (neg .z.w)(`upd;t;x)
	async_call := "(neg .z.w)(`upd;`" + table + ";" + query + ")"
	logger.Debug("AsyncCall: %v", async_call)
	this.Lock()
	err := this.Connection.AsyncCall(async_call);
	this.Unlock()

	if err != nil {
		logger.Error("Kdb query error, query: %v, error: %v", query, err)
		return errors.New("kdb query error")
	}
	return nil
}

func (this *Kdb) QueryNoneKeydTable2(query string, factory Factory_New) ([]interface{}, error) {
	if this.Connection == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	this.Lock()
	res, err := this.Connection.Call(query);
	this.Unlock()

	if err != nil {
		logger.Error("Kdb query error, query: %v, error: %v", query, err)
		return nil, errors.New("kdb query error")
	} else {
		// parse result
		table := res.Data.(kdb.Table)
		length := table.Data[0].Len()
		table_data := make([]interface{}, length, length)

		for i := 0; i < length; i++ {
			row := factory()
			if err := kdb.UnmarshalDict(table.Index(i), row); err != nil {
				return nil, err
			}
			table_data[i] = row
		}
		return table_data, nil
	}
}

func (this *Kdb) AsyncFuncTable(func_name string, table_name string, data interface{}) error {
	if table, err := Slice2KTable(data); err == nil {
		//logger.Debug("table: %v", table)
		k_tab := &kdb.K{kdb.XT, kdb.NONE, table}

		//this.Lock()
		err := this.Connection.AsyncCall(func_name, &kdb.K{-kdb.KS, kdb.NONE, table_name}, k_tab);
		//this.Unlock()

		if err != nil {
			logger.Error("Execute kdb function failed, func_name: %v, table_name: %v, error: %v",
				func_name, table_name, err)
			return  errors.New("Execute kdb function failed")
		} else {
			return nil
		}
	} else {
		return errors.New("Slice2KTable error")
	}
}

func (this *Kdb) FuncTable(func_name string, table_name string, data interface{}) (interface{}, error) {
	if table, err := Slice2KTable(data); err == nil {
		//logger.Debug("table: %v", table)
		k_tab := &kdb.K{kdb.XT, kdb.NONE, table}

		this.Lock()
		ret, err := this.Connection.Call(func_name, &kdb.K{-kdb.KS, kdb.NONE, table_name}, k_tab);
		this.Unlock()

		if err != nil {
			logger.Error("Execute kdb function failed, func_name: %v, table_name: %v, error: %v, return: %v",
				func_name, table_name, err, ret)
			return nil, errors.New("Execute kdb function failed")
		} else {
			return nil, nil
		}
	} else {
		return nil, errors.New("Slice2KTable error")
	}
}





func MarshalTable(v interface{}, table kdb.Table) (e1 error, t kdb.Table) {
	var err error = nil
	var keys = []string{}
	var values = []*kdb.K{}
	vv := reflect.ValueOf(v)
	aa := reflect.ValueOf(v).Elem()
	if vv.Kind() != reflect.Ptr || vv.IsNil() {
		err = errors.New("Invalid target type. Should be non null pointer")
	}
	vv = reflect.Indirect(vv)

	for i := 0; i < vv.NumField(); i++ {
		//		fmt.Println("vv.Field(k)", vv.Field(k))
		//		fmt.Printf("%s -- %v \n", vv.Type().Field(k).Name, vv.Field(k).Kind().String())

		switch vv.Field(i).Kind() {
		case reflect.Struct: {
			fmt.Println("struct===================")
			typ := vv.Field(i).Type()
			if vv.Field(i).NumField() == 3 && typ.Field(0).Name == "sec" && typ.Field(1).Name == "nsec" {
				//m := vv.Field(k).MethodByName("Local")
				//rets := m.Call([]reflect.Value{})
				/**
				var t time.Time = rets[0].Interface().(time.Time)
				m2 := vv.Field(k).MethodByName("Location")
				rets2 := m2.Call([]reflect.Value{})
				var l *time.Location = rets2[0].Interface().(*time.Location)
				// var timeFloat64 float64 = getNumDate(t, l)
				**/
				var timeFloat64 float64 = 1
				fmt.Println("timeFloat64,", timeFloat64)
				keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
				var tempk = &kdb.K{kdb.KZ, kdb.NONE, []float64{timeFloat64}}
				values = append(values, tempk)

			}
			//fallthrough
		}
		case reflect.Int32: {
			keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
			var tempk = &kdb.K{kdb.KI, kdb.NONE, []int32{aa.Field(i).Interface().(int32)}}
			values = append(values, tempk)
		}
		case reflect.Int64: {
			fmt.Println("int64===================")
			keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
			var tempk = &kdb.K{kdb.KI, kdb.NONE, []int32{aa.Field(i).Interface().(int32)}}
			values = append(values, tempk)
		}
		case reflect.Float32: {
			keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
			var tempk = &kdb.K{kdb.KF, kdb.NONE, []float32{aa.Field(i).Interface().(float32)}}
			values = append(values, tempk)
		}
		case reflect.Float64: {
			if vv.Field(i).Kind() == reflect.Float64 {
				keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
				var tempk = &kdb.K{kdb.KF, kdb.NONE, []float64{aa.Field(i).Interface().(float64)}}
				values = append(values, tempk)
			}
		}
		case reflect.Bool: {
			keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
			var tempk = &kdb.K{kdb.KF, kdb.NONE, []bool{aa.Field(i).Interface().(bool)}}
			values = append(values, tempk)
		}
		case reflect.String: {
			keys = append(keys, strings.ToLower(vv.Type().Field(i).Name))
			var tempk = &kdb.K{kdb.KS, kdb.NONE, []string{aa.Field(i).Interface().(string)}}
			values = append(values, tempk)
		}
		}
	}
	table.Columns = keys
	table.Data = values
	return err, table
}

func Slice2KTable(data interface{}) (kdb.Table, error) {
	var table kdb.Table
	data_value := reflect.ValueOf(data)
	var keys = []string{}
	var values = []*kdb.K{}
	if data_value.Len() > 0 {
		num_field :=data_value.Index(0).Type().NumField()
		// logger.Debug("num_field: %v", num_field)
		for i := 0; i < num_field ; i++ {
			//if i == 1 {
			//	continue
			//}
			col_name := data_value.Index(0).Type().Field(i).Name
			//fmt.Println(col_name)
			// logger.Debug(col_name)
			// keys = append(keys, strings.ToLower(col_name))
			keys = append(keys, strings.ToLower(col_name[0:1])+col_name[1:])
			//kind := data_value.Index(0).Field(i).Kind()
			tp:=data_value.Index(0).Field(i).Interface()
			//fmt.Println("kind",data_value.Index(0).Field(i).Kind())
			//fmt.Println("type",data_value.Index(0).Field(i).Type())
			switch tp.(type){
			case int32:{
				var col_data = []int32{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(int32))
				}
				col_data_k := &kdb.K{kdb.KI, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case string:{
				var col_data = []string{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(string))
				}
				col_data_k := &kdb.K{kdb.KS, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case int64: {
				var col_data = []int64{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(int64))
				}
				col_data_k := &kdb.K{kdb.KI, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case int: {
				var col_data = []int32{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, int32(data_value.Index(j).Field(i).Interface().(int)))
				}
				col_data_k := &kdb.K{kdb.KI, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			//byte
			case uint8:
				var col_data = []byte{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data,data_value.Index(j).Field(i).Interface().(byte))

				}
				col_data_k := &kdb.K{kdb.KC, kdb.NONE, string(col_data)}
				values = append(values, col_data_k)
			case float32: {
				var col_data = []float32{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(float32))
				}
				col_data_k := &kdb.K{kdb.KF, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case float64: {
				var col_data = []float64{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(float64))
				}
				col_data_k := &kdb.K{kdb.KF, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case bool: {
				var col_data = []bool{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(bool))
				}
				col_data_k := &kdb.K{kdb.KB, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case time.Time:
				var col_data = []float64{}
				for j := 0; j < data_value.Len(); j++ {
					m := data_value.Index(j).Field(i).MethodByName("Local")
					rets := m.Call([]reflect.Value{})
					var t time.Time = rets[0].Interface().(time.Time)
					m2 := data_value.Index(j).Field(i).MethodByName("Location")
					rets2 := m2.Call([]reflect.Value{})
					var l *time.Location = rets2[0].Interface().(*time.Location)
					var timeFloat64 float64 = getNumDate(t, l)
					col_data = append(col_data, timeFloat64)
				}
				col_data_k := &kdb.K{kdb.KZ, kdb.NONE, col_data}
				values = append(values, col_data_k)

			case kdb.Minute:
				var col_data = []int32{}
				for j := 0; j < data_value.Len(); j++ {
					v_kdb := data_value.Index(j).Field(i).Interface().(kdb.Minute)
					//logger.Debug("v_kdb minute: %v", v_kdb)
					tt := time.Time(v_kdb)
					dur := tt.Sub(time.Time{})
					//logger.Debug("Duration: %v, minutes: %v", dur, dur.Minutes())
					col_data = append(col_data, int32(dur.Minutes()))
				}
				col_data_k := &kdb.K{kdb.KU, kdb.NONE, col_data}
				values = append(values, col_data_k)
			case kdb.Second:
				var col_data = []int32{}
				for j := 0; j < data_value.Len(); j++ {
					v_kdb := data_value.Index(j).Field(i).Interface().(kdb.Second)
					//logger.Debug("v_kdb second: %v", v_kdb)
					tt := time.Time(v_kdb)
					dur := tt.Sub(time.Time{})
					//logger.Debug("Duration: %v, seconds: %v", dur, dur.Seconds())
					col_data = append(col_data, int32(dur.Seconds()))
				}
				col_data_k := &kdb.K{kdb.KV, kdb.NONE, col_data}
				values = append(values, col_data_k)

			case kdb.Time:
				var col_data = []int64{}
				qEpoch := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
				for j := 0; j < data_value.Len(); j++ {
					v_kdb := data_value.Index(0).Field(i).Interface().(kdb.Time)
					//logger.Debug("v_kdb time: %v", v_kdb)
					tt := time.Time(v_kdb)
					dur := tt.Sub(qEpoch)
					//logger.Debug("Duration: %v, Millisecond: %v", dur, dur.Seconds()*1000)
					col_data = append(col_data, int64(dur.Seconds()*1000))
				}
				col_data_k := &kdb.K{kdb.KT, kdb.NONE, col_data}
				values = append(values, col_data_k)
			case time.Duration:
				var col_data=[]time.Duration{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(time.Duration))
				}
				col_data_k := &kdb.K{kdb.KN, kdb.NONE, col_data}
				values = append(values, col_data_k)

			default:
				logger.Error("Unkonwn struct,data: %V name: %v type: %v ",data_value.Index(0), data_value.Index(0).Field(i),data_value.Index(0).Field(i).Type())
				//return table, errors.New("Unkown struct")

			}

		}
		//logger.Debug("keys: %v, values: %v", keys, values)

	}
	table.Columns = keys
	table.Data = values
	return table, nil
}

// TODO
func getNumDate(datetime time.Time, local *time.Location) float64 {
	var qEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, local)
	diff := ((float64)(datetime.UnixNano() - qEpoch.UnixNano()) / (float64)(864000 * 100000000))
	return diff
}

