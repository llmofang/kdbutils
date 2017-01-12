package kdbutils

import (
	kdb "github.com/sv/kdbgo"
	logger "github.com/alecthomas/log4go"
	"fmt"
	"errors"
	"reflect"
	"strings"
	"time"
)

// ref: http://stackoverflow.com/questions/10210188/instance-new-type-golang

type Factory_New func() interface{}

type Kdb struct {
	Host       string
	Port       int
	Connection *kdb.KDBConn
}

func MewKdb(host string, port int) *Kdb {
	kdb := &Kdb{host, port, nil}
	return kdb
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
	if this.Connection == nil {
		return true
	} else {
		return false
	}
}

func (this *Kdb) Disconnect() error {
	logger.Info("disconnecting to kdb, host: %v, port:%v", this.Host, this.Port)
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
	logger.Info("Subscribing Kdb, table: %s, sym: %v, sym_num: %v", table, sym, sym_num)
	var err error
	if sym_num == 0 {
		err = this.Connection.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{-kdb.KS, kdb.NONE, ""})
	} else {
		err = this.Connection.AsyncCall(".u.sub", &kdb.K{-kdb.KS, kdb.NONE, table},
			&kdb.K{kdb.KS, kdb.NONE, sym})
	}
	if err != nil {
		logger.Error("Failed to subscibe, table: %s, sym; %s", table, sym)
	}
}

func (this *Kdb) SubscribedData2Channel(channel chan <-interface{}, table2struct map[string]Factory_New) {
	for {
		// ignore type print output
		res, _, err := this.Connection.ReadMessage()
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
		logger.Debug("message's content: %d", table_data)
		for i := 0; i < length; i++ {
			row := factory()
			err := kdb.UnmarshalDict(table_data.Index(i), row)
			if err != nil {
				fmt.Println("Failed to unmrshall dict ", err)
				continue
			}
			logger.Debug("before send: %v", row)
			channel <- row
		}
	}
}

func (this *Kdb) QueryNoneKeyedTable(query string, v interface{}) (interface{}, error) {
	if this.Connection == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	if res, err := this.Connection.Call(query); err != nil {
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
	if this.Connection == nil {
		logger.Error("Kdb is not connected")
		return nil, errors.New("kdb is not connected")
	}

	if res, err := this.Connection.Call(query); err != nil {
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
	if table, err := Slice2KTable(data); err == nil {
		//logger.Debug("table: %v", table)
		k_tab := &kdb.K{kdb.XT, kdb.NONE, table}
		if ret, err := this.Connection.Call(func_name, &kdb.K{-kdb.KS, kdb.NONE, table_name}, k_tab); err != nil {
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
			// logger.Debug(col_name)
			keys = append(keys, strings.ToLower(col_name))
			kind := data_value.Index(0).Field(i).Kind()
			// logger.Debug(kind)

			switch kind {

			case reflect.Int32: {
				var col_data = []int32{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(int32))
				}
				col_data_k := &kdb.K{kdb.KI, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.Int64: {
				var col_data = []int64{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(int64))
				}
				col_data_k := &kdb.K{kdb.KI, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.Float32: {
				var col_data = []float32{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(float32))
				}
				col_data_k := &kdb.K{kdb.KF, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.Float64: {
				var col_data = []float64{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(float64))
				}
				col_data_k := &kdb.K{kdb.KF, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.Bool: {
				var col_data = []bool{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(bool))
				}
				col_data_k := &kdb.K{kdb.KB, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.String: {
				var col_data = []string{}
				for j := 0; j < data_value.Len(); j++ {
					col_data = append(col_data, data_value.Index(j).Field(i).Interface().(string))
				}
				col_data_k := &kdb.K{kdb.KS, kdb.NONE, col_data}
				values = append(values, col_data_k)
			}
			case reflect.Struct: {
				v := data_value.Index(0).Field(i).Interface()
				switch t:= v.(type) {
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

				default:
					logger.Error("Unkonwn struct, type: %v", t)
					return table, errors.New("Unkown struct")

				}

			}
			}
			//logger.Debug("keys: %v, values: %v", keys, values)

		}
	}
	table.Columns = keys
	table.Data = values
	return table, nil
}

