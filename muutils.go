package revent_sdk_go

import "sync"

type genericRWMutex[T any] struct {
	mu  sync.RWMutex
	val T
}

func newGenericRWMutex[T any](val T) *genericRWMutex[T] {
	return &genericRWMutex[T]{val: val}
}

func (g *genericRWMutex[T]) get() T {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.val
}

func (g *genericRWMutex[T]) set(val T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.val = val
}
