package resp

import (
	"container/heap"
	"sync"
)

type PQItem struct {
	Key string
	Value int64
}

type PriorityQueue []PQItem

func (pq PriorityQueue) Len() int 		{ return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool 	{ return pq[i].Value < pq[j].Value }
func (pq PriorityQueue) Swap(i, j int) 		{ pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(PQItem))
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[:n-1]
	return x
}

func (pq PriorityQueue) Peek() any {
	if pq.Len() == 0 {
		return nil
	}
	return pq[0]
}

type SafePriorityQueue struct {
	mu sync.RWMutex
	pq PriorityQueue
}

func NewSafePriorityQueue() *SafePriorityQueue {
	return &SafePriorityQueue{pq: PriorityQueue{}}
}

func (spq *SafePriorityQueue) Push(x any) {
	spq.mu.Lock()
	defer spq.mu.Unlock()

	heap.Push(&spq.pq, x)
}

func (spq *SafePriorityQueue) Pop() any {
	spq.mu.Lock()
	defer spq.mu.Unlock()

	return heap.Pop(&spq.pq)
}

func (spq *SafePriorityQueue) Peek() any {
	spq.mu.RLock()
	defer spq.mu.RUnlock()

	return spq.pq.Peek()
}
