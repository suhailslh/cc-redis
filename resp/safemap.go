package resp

import (
	"sync"
)

type SafeMap struct {
	mu sync.RWMutex
	items map[string]DataType
}

func NewSafeMap() *SafeMap {
	return &SafeMap{items: make(map[string]DataType)}
}

func (s *SafeMap) Write(key string, value DataType) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if value == nil {
		delete(s.items, key)
		return
	}

	s.items[key] = value
}

func (s *SafeMap) Read(key string) DataType {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.items[key]
}
