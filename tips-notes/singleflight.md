# Singleflight: A Solution for Eliminating Redundant Work

Source:

- <https://www.codingexplorations.com/blog/understanding-singleflight-in-golang-a-solution-for-eliminating-redundant-work>
- <https://victoriametrics.com/blog/go-singleflight/>

Scenario: when you've got multiple requests coming in at the same time asking for the **same data**, the default behavior is that each of those requests would go to the database individually to get the same information -> execute the same query several times -> inefficient. That's called [thundering herd problem](https://en.wikipedia.org/wiki/Thundering_herd_problem).

![](https://victoriametrics.com/blog/go-singleflight/go-singleflight-unuse.webp)

The idea is that only the first request actually goes to the database. The rest of the request wait for the first one to finish. Once the data comes back from the initial requests, the other ones just get the same result - no extra queries needed.

![](https://victoriametrics.com/blog/go-singleflight/go-singleflight-overview.webp)

Implement:

```go
package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

var (
	callCount atomic.Int32
	wg        sync.WaitGroup
)

// Simulate a function that fetches data from a database
func fetchData() (interface{}, error) {
	callCount.Add(1)
	time.Sleep(100 * time.Millisecond)
	return rand.IntN(100), nil
}

// Wrap the fetchData function with singleflight
func fetchDataWrapper(g *singleflight.Group, id int) error {
	defer wg.Done()

	time.Sleep(time.Duration(id) * 40 * time.Millisecond)
    // assign a unique key to track these request. So, if another goroutine
    // asks for the same key while the first one is still fetching the data, it waits
    // for the result rather than starting a new call.
	v, err, shared := g.Do("key-fetch-data", fetchData)
	if err != nil {
		return err
	}

	fmt.Printf("Goroutine %d: result: %v, shared: %v\n", id, v, shared)
	return nil
}

func main() {
	var g singleflight.Group

	// 5 goroutines to fetch the same data
	const numGoroutines = 5
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go fetchDataWrapper(&g, i)
	}

	wg.Wait()
	fmt.Printf("Function was called %d times\n", callCount.Load())
}
// Goroutine 0: result: 90, shared: true
// Goroutine 2: result: 90, shared: true
// Goroutine 1: result: 90, shared: true
// Goroutine 3: result: 13, shared: true
// Goroutine 4: result: 13, shared: true
// Function was called 2 times
```

![](https://victoriametrics.com/blog/go-singleflight/go-singleflight-demo.webp)

- Once the first call finishes, any waiting goroutines get the same result.
- 5 goroutines - fetchData only ran twice.
- The shared flag confirms that the result was reused across multiple goroutines. The thing is, the shared variable in g.Do tells you whether the result was shared among multiple callers. It’s basically saying, “Hey, this result was used by more than one caller.” It’s not about who ran the function, it’s just a signal that the result was reused across multiple goroutines.

> I have a cache, why do I need singleflight

The short answer is: caches and singleflight solve different problems, and they actually work really well together.

![](https://victoriametrics.com/blog/go-singleflight/go-singleflight-cache.webp)

In addition, singleflight helps protect against a cache miss storm ("cache stampede").

- When a request asks for data, if the data is in the cache -> cache hit.
- If the data isn't in the cache, it's a cache miss.
- Suppose 10,000 requests hit the system all at once before the cache is rebuilt, the database could suddenly get slammed with 10,000 identical queries at the same time -> During this peak, singleflight ensures that only one of those 10,000 requests actually hits the database.

Singleflight uses a global lock to protect the map of in-flight calls, which can become a single point of contention for every goroutine. This can slow things down, especially if you're dealing with high concurrency.

The model below might work better for machines with multiple CPUs:

![](https://victoriametrics.com/blog/go-singleflight/go-singleflight-cache-v2.webp)

Real-world scenarios:

- The first goroutine might take way longer than expected due to things like slow network responses, unresponsive databases, etc. In that case, all the other waiting goroutines are stuck for longer than you’d like. A timeout can help here, but any new requests will still end up waiting behind the first one.
- The data you’re fetching might change frequently, so by the time the first request finishes, the result could be outdated. That means we need a way to invalidate the key and trigger a new execution -> singleflight `group.Forget(key)`.

```go
func fetchDataWrapperWithForget(g *singleflight.Group, id int, forget bool) error {
	defer wg.Done()

	// Forget the key before fetching
	if forget {
		g.Forget("key-fetch-data")
	}

	v, err, shared := g.Do("key-fetch-data", fetchData)
	if err != nil {
		return err
	}

	fmt.Printf("Goroutine %d: result: %v, shared: %v\n", id, v, shared)
	return nil
}

func main() {
    wg.Add(3)

	// 2 goroutines fetch the data
	go fetchDataWrapperWithForget(&g, 0, false)
	go fetchDataWrapperWithForget(&g, 1, false)

	// Wait a bit and launch 1 more goroutine
	// Ensures goroutines 0, 1, and 2 overlap
	time.Sleep(10 * time.Millisecond)
	go fetchDataWrapperWithForget(&g, 2, true)

	wg.Wait()
	fmt.Printf("Function was called %d times\n", callCount.Load())

// Output:
// Goroutine 0: result: 55, shared: true
// Goroutine 1: result: 55, shared: true
// Goroutine 2: result: 73, shared: false
// Function was called 2 times
}
```

To sum up, while singleflight is useful, it can still have some edge cases, for example:

- If the first goroutine gets blocked for too long, all the others waiting on it will also be stuck. In such cases, using a timeout context or a select statement with a timeout can be a better option.
- If the first request returns an error or panics, that same error or panic will propagate to all the other goroutines waiting for the result.
