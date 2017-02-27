package comm

import (
	"sync"
	// "github.com/alecthomas/log4go"
)

// 订阅者结构，包含订阅数量的计数
type Subscriber struct {
	Set map[string]int
	sync.RWMutex
}

// 订阅
// 返回是否要更新kdb的keys
// 如果不需要更新kdb则返回nil
func (this *Subscriber) Subscribe(key string) []string {
	this.Lock()
	defer this.Unlock()

	rtn := this.sub(key)
	if rtn > 1 {
		return nil
	}

	return this.ToSlice()
}

// 退订
// 返回是否要更新kdb的keys
// 如果不需要更新kdb则返回nil
func (this *Subscriber) Unsubscribe(key string) []string {
	this.Lock()
	defer this.Unlock()

	rtn := this.unsub(key)
	if !rtn {
		return nil
	}

	return this.ToSlice()
}

// 订阅
// 如果存在，增加计数
// 如果不存在，增加映射
func (this *Subscriber) sub(key string) int {
	if _, found := this.Set[key]; found {
		this.Set[key] += 1
	} else {
		this.Set[key] = 1
	}

	return this.Set[key]
}

// 退订
// 如果存在，递减计数，如果递减为0，删除映射
// 如果不存在，增加映射
// 返回是否改变映射关系
func (this *Subscriber) unsub(key string) bool {
	changes := false
	if _, found := this.Set[key]; found {
		// log4go.Debug("set: %v", this.Set)
		this.Set[key] -= 1
		// log4go.Debug("set: %v", this.Set)
		if this.Set[key] < 1 {
			delete(this.Set, key)
			changes = true
		}
	}
	return changes
}

// 将keys转化为slice
func (this *Subscriber) ToSlice() []string {
	keys := make([]string, 0, 50)

	for elem := range this.Set {
		keys = append(keys, elem)
	}

	return keys
}

