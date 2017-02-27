package connectionpool

import (
	"container/list"
	"sync"
)

type Stack struct {
	list *list.List
	sync.RWMutex
}

func NewStack() *Stack {
	list := list.New()
	stack:=Stack{}
	stack.list=list
	return &stack
}

func (stack *Stack) Push(value interface{}) {
	stack.Lock()
	stack.list.PushBack(value)
	stack.Unlock()
}

func (stack *Stack) Pop() interface{} {
	e := stack.list.Back()
	if e != nil {
		stack.Lock()
		stack.list.Remove(e)
		stack.Unlock()
		return e.Value
	}
	return nil
}

func (stack *Stack) Peak() interface{} {
	e := stack.list.Back()
	if e != nil {
		return e.Value
	}

	return nil
}

func (stack *Stack) Len() int {
	return stack.list.Len()
}

func (stack *Stack) Empty() bool {
	return stack.list.Len() == 0
}
