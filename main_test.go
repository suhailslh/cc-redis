package main

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRunConcurrent(t *testing.T) {
	ready := make(chan bool, 1)
	interrupt := make(chan os.Signal, 1)
	go func() {
		run(ready, interrupt)
	}()
	defer func() { interrupt <- os.Interrupt }()
	
	<-ready

	ctx := context.Background()	
	
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rdb := redis.NewClient(&redis.Options{
				Addr:     "0.0.0.0:36245",
				Password: "",
				DB:       0,
				Protocol: 2,
			})
			defer rdb.Close()

			expected := "bar"
			err := rdb.Set(ctx, "foo", expected, 0).Err()
			if err != nil {
				t.Fatal(err)
			}

			actual, err := rdb.Get(ctx, "foo").Result()
			if err != nil {
				t.Fatal(err)
			}
			
			if actual != expected {
				t.Errorf("%d: expected %q; actual %q", i, expected, actual)
			}
		}()
	}
	wg.Wait()
}
