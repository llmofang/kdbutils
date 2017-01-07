package kdbutils

import (
	kdb "github.com/sv/kdbgo"
	logger "github.com/alecthomas/log4go"
	"fmt"
	"errors"
	"reflect"
	"strings"
)

// ref: http://stackoverflow.com/questions/10210188/instance-new-type-golang

type Factory_New func() interface{}

type Kdb struct {
	host string
	post int
	con  *kdb.KDBConn
}

func MewKdb(host string, port int) *Kdb {
	kdb := &Kdb{host, port, nil}
	return kdb
}

func (this *Kdb) Connect() error {
	logger.Info("connecting to kdb, host: %v, port:%v", this.host, this.post)
	var err error
	if this.con, err = kdb.DialKDB(this.host, this.post, "kdbgo:kdbgo"); err == nil {
		logger.Info("Connected to kdb successful")
	} else {
		logger.Error("Failed to connect to kdb, error: %s", err.Error())
		return err
	}
	return nil
}

func (this *Kdb) Disconnect() error {
	logger.Info("disconnecting to kdb, host: %v, port:%v", this.host, this.post)
	err := this.con.Close()
	if err == nil {
		logger.Info("disconnected to kdb successful")
	} else {
		logger.Error("disconnected to kdb error, err: %s", err.Error())
	}
	return err
}

func (this *Kdb) Subscribe(table string, sym []string) {
	if this.con == nil {
		this.Connect()
	}
	sym_num := len(sym)
	logger.Info("Subscribing Kdb, table: %s, sym: %v, sym_num: %v", table, sym, sym_num)
	var err error
	if sym_num == 0 {
		err = this.con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{-kdb.KS, kdb.NONE, ""})
	} else {
		err = this.con.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{kdb.KS, kdb.NONE, sym})
	}
	if err != nil {
		logger.Error("Failed to subscibe, table: %s, sym; %s", table, sym)
	}
}

func (this *Kdb) SubscribedData2Channel(channel chan <-interface{}, table2struct map[string]Factory_New) {
	for {
		// ignore type print output
		res, _, err := this.con.ReadMessage()
		if err != nil {
			logger.Error("Error on processing KDB message, error: ", err.Error())
			return
		}
		len := res.Len()
		if len != 3 {
			logger.Error("Message is not pub data, length: %i", len)
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
		logger.Debug("message's table_name: %s", table_name)

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
			continue
		}

		table_data := data_list[2].Data.(kdb.Table)
		length := table_data.Data[0].Len()
		logger.Debug("message's length: %d", length)
		for i := 0; i < length; i++ {
			row := factory()
			err := kdb.UnmarshalDict(table_data.Index(i), row)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			channel <- row
		}
	}
}

func (this *Kdb) QueryNoneKeyedTable(query string, v interface{}) (interface{}, error) {
	if this.con == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	if res, err := this.con.Call(query); err != nil {
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

func (this *Kdb) QueryNoneKeydTable2(query string, factory Factory_New) ([]interface{}, error) {
	if this.con == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	if res, err := this.con.Call(query); err != nil {
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

func (this *Kdb) FuncTable(func_name string, table_name string, data interface{}) (interface{}, error) {
	table := kdb.Table{}
	if err, table := MarshalTable(data, table); err == nil {
		logger.Error("MarshalTable error, Err: %v", err)
		return nil, errors.New("MarshalTable error")
	} else {
		k_tab := &kdb.K{kdb.XT, kdb.NONE, table}
		if ret, err := this.con.Call(func_name, &kdb.K{-kdb.KS, kdb.NONE, table_name}, k_tab); err == nil {
			logger.Debug("Execute kdb function successfully, func_name: %v, table_name: %v, return: %v",
				func_name, table_name, ret)
			return  ret, nil
		} else {
			logger.Debug("Execute kdb function failed, func_name: %v, table_name: %v",
				func_name, table_name)
			return nil, errors.New("Execute kdb function failed")
		}
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


