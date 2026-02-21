package resp

import (
	"sync"
)

type SafeMap[K comparable, V any] struct {
	mu sync.RWMutex
	items map[K]V
}

func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{items: make(map[K]V)}
}

func (s *SafeMap[K, V]) Write(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = value
}

func (s *SafeMap[K, V]) Read(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.items[key]
	return value, ok
}

func (s *SafeMap[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.items, key)
}
