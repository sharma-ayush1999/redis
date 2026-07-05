package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main(){
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to redis: %v", err)
	}

	fmt.Println("connected to redis")

	basicCommands(rdb)
	proveSingleThreaded()
}

func basicCommands(rdb *redis.Client){
	fmt.Println("\n-- basic commands --")

	rdb.Set(ctx, "user:1:name", "ayush", 0)
	name, _ := rdb.Get(ctx, "user:1:name").Result()
	fmt.Println("GET user:1:name", name)

	// TTL / expiration
	rdb.Set(ctx, "otp:1", "345234", 30 * time.Second)
	ttl, _ := rdb.TTL(ctx, "otp:1").Result()
	fmt.Println("TTL otp:1", ttl)

	// atomic increment — common interview point: no race condition needed
	// because Redis executes each command atomically on its single thread
	rdb.Del(ctx, "views:page1")
	for range 5 {
		rdb.Incr(ctx, "views:page1")
	}

	views, _ := rdb.Get(ctx, "views:page1").Result()
	fmt.Println("views:page1 after 5 concurrent-safe INCRs =>", views)

	rdb.Del(ctx, "user:1:name", "otp:1")
}

// proveSingleThreaded demonstrates the classic interview point: a single
// slow/blocking command stalls ALL other clients, because command execution
// happens on one thread. We fire a fast GET concurrently while a blocking
// command (DEBUG SLEEP) runs, and time how long the "fast" command waits.
func proveSingleThreaded(){
	fmt.Println("\n-- proving single-threaded execution --")
	rdb2 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		ReadTimeout: 20 * time.Second,
	})
	rdb3 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		ReadTimeout: 20 * time.Second,
	})
	defer rdb2.Close()
	defer rdb3.Close()

	rdb2.Ping(ctx)
	rdb3.Ping(ctx)

	done := make(chan time.Duration)

	// Client A: issues a command that blocks the server for 2s.
	// go func(){
	// 	start := time.Now()
	// 	res, err := rdb2.Do(ctx, "DEBUG", "SLEEP", "2").Result()
	// 	fmt.Printf("DEBUG SLEEP returned after %v — result: %v, err: %v\n", time.Since(start), res, err)
	// }()

	go func(){
		start := time.Now()
		script := `local i = 0 while i < 1000000000 do i = i + 1 end return i`
		res, err := rdb2.Eval(ctx, script, []string{}).Result()
		fmt.Printf("EVAL returned after %v — result: %v, err: %v\n", time.Since(start), res, err)
	}()

	time.Sleep(20 * time.Millisecond)

	// Client B: a trivially fast GET, issued while the server is "asleep".
	go func(){
		start := time.Now()
		rdb3.Get(ctx, "nonexistent-key").Result()
		done <- time.Since(start)
	}()

	elapsed := <-done
	fmt.Printf("fast GET took %v while server was busy — proves single-threaded blocking\n", elapsed)
	fmt.Println("(expect ~1.8s, NOT ~0ms, since DEBUG SLEEP blocks the single command-execution thread)")

}