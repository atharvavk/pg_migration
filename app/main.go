package main

// Switchover load generator.
//
// Spawns -rate goroutines per second (default 5/s = 300/min). Each goroutine:
//   BEGIN
//   INSERT INTO names (first_name, last_name) -> RETURNING id
//   SELECT inet_server_port()   (records which postgres instance served it)
//   sleep 10s                    (transaction held open the whole time)
//   UPDATE names SET last_name = last_name || '2' WHERE id = $1
//   COMMIT
//
// Fail-safe libpq driver (no cached prepared statements) => safe under
// pgbouncer transaction pooling.
//
// Exits 0 only if every transaction committed. Prints a report split by
// backend port so you can see traffic move 5432 -> 5433 at switchover.

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

const txTimeout = 150 * time.Second // covers buffering during a long pause

// wall-clock test start; used for the 10s timeline buckets
var runStart time.Time

var firstNames = []string{"Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Linus", "Margaret", "Dennis", "Katherine"}
var lastNames = []string{"Lovelace", "Hopper", "Turing", "Dijkstra", "Liskov", "Knuth", "Torvalds", "Hamilton", "Ritchie", "Johnson"}

type report struct {
	total, completed, failed int
	okByPort                 map[string]int
	failures                 []string
	spawnToInsert            []time.Duration        // buffering evidence
	timeline                 map[int]map[string]int // 10s bucket -> port -> committed txs
	mu                       sync.Mutex
}

func (r *report) recordPort(port string, elapsed time.Duration) {
	r.mu.Lock()
	r.okByPort[port]++
	b := int(time.Since(runStart) / (10 * time.Second))
	if r.timeline[b] == nil {
		r.timeline[b] = map[string]int{}
	}
	r.timeline[b][port]++
	r.mu.Unlock()
}

func (r *report) recordFail(phase string, err error) {
	r.mu.Lock()
	r.failures = append(r.failures, fmt.Sprintf("%s phase: %v", phase, err))
	r.mu.Unlock()
}

func (r *report) recordWait(d time.Duration) {
	r.mu.Lock()
	r.spawnToInsert = append(r.spawnToInsert, d)
	r.mu.Unlock()
}

func worker(db *sql.DB, wg *sync.WaitGroup, rep *report, verbose bool) {
	defer wg.Done()
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		rep.recordFail("BEGIN", err)
		return
	}

	var id int64
	err = tx.QueryRowContext(ctx,
		"INSERT INTO names (first_name, last_name) VALUES ($1, $2) RETURNING id",
		firstNames[rand.Intn(len(firstNames))],
		lastNames[rand.Intn(len(lastNames))],
	).Scan(&id)
	if err != nil {
		_ = tx.Rollback()
		rep.recordFail("INSERT", err)
		return
	}
	rep.recordWait(time.Since(t0))

	// tag which postgres instance actually executed this transaction; never fatal
	var port sql.NullString
	if e := tx.QueryRowContext(ctx, "SELECT inet_server_port()").Scan(&port); e != nil {
		port = sql.NullString{String: "unknown", Valid: true}
	}
	target := port.String
	if !port.Valid || target == "" {
		target = "unix-socket?"
	}

	time.Sleep(10 * time.Second)

	if _, err = tx.ExecContext(ctx,
		"UPDATE names SET last_name = last_name || '2' WHERE id = $1", id); err != nil {
		_ = tx.Rollback()
		rep.recordFail("UPDATE", err)
		if verbose {
			fmt.Printf("[%s] FAILED %s phase: %v\n", time.Now().Format("15:04:05"), target, err)
		}
		return
	}
	if err = tx.Commit(); err != nil {
		rep.recordFail("COMMIT", err)
		if verbose {
			fmt.Printf("[%s] FAILED %s phase: %v\n", time.Now().Format("15:04:05"), target, err)
		}
		return
	}
	rep.recordPort(target, time.Since(t0))
	if verbose {
		fmt.Printf("[%s] ok    id=%-4d %-15s total %s  spawn->insert %s\n",
			time.Now().Format("15:04:05"), id, target, time.Since(t0).Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func main() {
	rate := flag.Int("rate", 5, "goroutines spawned per second")
	duration := flag.Duration("duration", 60*time.Second, "how long to generate load")
	dsn := flag.String("dsn", "host=127.0.0.1 port=6432 dbname=appdb sslmode=disable", "pgbouncer DSN (user defaults to the OS user)")
	verbose := flag.Bool("v", false, "print every transaction result")
	flag.Parse()

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(400)
	db.SetMaxIdleConns(50) // reduce churn; keeps conns warm for pause buffering

	rep := &report{okByPort: map[string]int{}, timeline: map[int]map[string]int{}}
	runStart = time.Now()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	spawn := time.NewTicker(time.Second / time.Duration(*rate))
	defer spawn.Stop()

	spawned := 0
loop:
	for {
		select {
		case <-spawn.C:
			spawned++
			wg.Add(1)
			go worker(db, &wg, rep, *verbose)
			if time.Duration(spawned)*time.Second/time.Duration(*rate) >= *duration {
				break loop
			}
		case <-stop:
			break loop
		}
	}
	wg.Wait() // let in-flight 10s transactions finish before reporting

	rep.mu.Lock()
	defer rep.mu.Unlock()
	rep.total = spawned
	rep.completed = sum(rep.okByPort)
	rep.failed = len(rep.failures)

	var waits []time.Duration
	waits = append(waits, rep.spawnToInsert...)
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	maxWait, p95 := waits[len(waits)-1], percentile(waits, 0.95)

	fmt.Println("\n================ SWITCHOVER TEST REPORT ================")
	fmt.Printf("spawned: %d   committed: %d   FAILED: %d\n", rep.total, rep.completed, rep.failed)
	ports := []string{}
	for p := range rep.okByPort {
		ports = append(ports, p)
	}
	sort.Strings(ports)
	for _, p := range ports {
		fmt.Printf("committed via port %-5s : %d transactions\n", p, rep.okByPort[p])
	}
	fmt.Println("\ncommitted txs per 10s bucket (t0 = first spawn, port shown as it was after commit):")
	buckets := []int{}
	for b := range rep.timeline {
		buckets = append(buckets, b)
	}
	sort.Ints(buckets)
	fmt.Printf("  %-14s %-8s %-8s\n", "10s-bucket", "5432", "5433")
	for _, b := range buckets {
		fmt.Printf("  %-14d %-8d %-8d\n", b, rep.timeline[b]["5432"], rep.timeline[b]["5433"])
	}
	if len(waits) > 0 {
		fmt.Printf("spawn->INSERT  p50=%s  p95=%s  max=%s  (max includes pause buffering)\n",
			percentile(waits, 0.5), p95, maxWait)
	}
	if rep.failed > 0 {
		fmt.Println("failures:")
		for i, f := range rep.failures {
			if i >= 10 {
				fmt.Printf("  ... and %d more\n", len(rep.failures)-10)
				break
			}
			fmt.Printf("  - %s\n", f)
		}
	}
	fmt.Println("========================================================")
	if rep.failed > 0 {
		os.Exit(1)
	}
}

func sum(m map[string]int) int {
	t := 0
	for _, v := range m {
		t += v
	}
	return t
}
