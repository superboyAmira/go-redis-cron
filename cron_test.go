package cron

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const OneSecond = 1*time.Second + 10*time.Millisecond

var pool = redisPool()

func redisPool() *redis.Client {
	redisAddr := ":6379"
	if v, ok := os.LookupEnv("REDIS_ADDR"); ok {
		redisAddr = v
	}
	redisUsr := ""
	if v, ok := os.LookupEnv("REDIS_USER"); ok {
		redisAddr = v
	}
	redisPass := ""
	if v, ok := os.LookupEnv("REDIS_PASS"); ok {
		redisAddr = v
	}
	redisCacheClient := redis.NewClient(
		&redis.Options{
			Addr:     redisAddr,
			Password: redisPass,
			Username: redisUsr,
			DB:       0, // use default DB
		})

	if err := redisCacheClient.Ping(context.Background()).Err(); err != nil {
		panic("failed to connect to redis server: " + err.Error())
	}
	return redisCacheClient
}

func TestFuncPanicRecovery(t *testing.T) {
	cron := New(context.Background(), pool)
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { panic("YOLO") })

	select {
	case <-time.After(OneSecond):
		return
	}
}

type concurrencyJob struct {
	name string
	wg   *sync.WaitGroup
}

func (c *concurrencyJob) Name() string {
	return c.name
}

func (c *concurrencyJob) Run() {
	c.wg.Done()
}

func TestOneJobRunTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New(context.Background(), pool)
	cron.AddJob("* * * * * ?", &concurrencyJob{name: "TestOneJobRunTwice", wg: wg})
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Fatal("the job did not run twice after 2 seconds.")
	case <-wait(wg):
	}
}

func TestRunningInConcurrency(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	for i := 0; i < 100; i++ {
		cron.AddJob("* * * * * ?", &concurrencyJob{name: "TestRunningInConcurrency", wg: wg})
	}
	cron.Start()
	defer cron.Stop()

	time.Sleep(time.Second)
	select {
	case <-time.After(OneSecond):
		t.Fatal("multi same jobs run more than one time")
	case <-wait(wg):
	}
}

func TestCalcExpiry(t *testing.T) {
	cron := New(context.Background(), pool)
	now := cron.now()
	next := now.Add(time.Second)
	expiry := cron.calcExpiry(now, next)
	if expiry <= 0 {
		t.Fatalf("expiry less than 0")
	}
}

type DummyJob struct{}

func (d DummyJob) Run() {
	panic("YOLO")
}

func (d DummyJob) Name() string {
	return "DummyJob"
}

func TestJobPanicRecovery(t *testing.T) {
	var job DummyJob

	cron := New(context.Background(), pool)
	cron.Start()
	defer cron.Stop()
	cron.AddJob("* * * * * ?", job)

	select {
	case <-time.After(OneSecond):
		return
	}
}

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := New(context.Background(), pool)
	cron.Start()

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected cron will be stopped immediately")
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		// No job ran!
	case <-wait(wg):
		t.Fatal("expected stopped cron does not run any job")
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Test for #34. Adding a job after calling start results in multiple job invocations
func TestAddWhileRunningWithDelay(t *testing.T) {
	cron := New(context.Background(), pool)
	cron.Start()
	defer cron.Stop()
	time.Sleep(5 * time.Second)
	var calls = 0
	cron.AddFunc("* * * * * *", func() { calls += 1 })

	<-time.After(OneSecond)
	if calls != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.AddFunc("@every 2s", func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(OneSecond):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(OneSecond):
		t.Error("expected job runs at 2 second mark")
	case <-wait(wg):
	}

}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New(context.Background(), pool)
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job run in proper order")
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New(context.Background(), pool)
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New(context.Background(), pool)
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.schedule(every(time.Minute), FuncJob(func() {}), "00 */1 * * * *")
	cron.schedule(every(time.Second), FuncJob(func() { wg.Done() }), "* * * * * *")
	cron.schedule(every(time.Hour), FuncJob(func() {}), "00 00 */1 * * *")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	now := time.Now()
	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New(context.Background(), pool)
	cron.AddFunc(spec, func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the given time zone (as opposed to local).
func TestNonLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	loc, err := time.LoadLocation("Atlantic/Cape_Verde")
	if err != nil {
		fmt.Printf("Failed to load time zone Atlantic/Cape_Verde: %+v", err)
		t.Fail()
	}

	now := time.Now().In(loc)
	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := NewWithLocation(context.Background(), loc, pool)
	cron.AddFunc(spec, func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that calling stop before start silently returns without
// blocking the stop channel.
func TestStopWithoutStart(t *testing.T) {
	cron := New(context.Background(), pool)
	cron.Stop()
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
}

func (t testJob) Name() string {
	return t.name
}

// Test that adding an invalid job spec returns an error
func TestInvalidJobSpec(t *testing.T) {
	cron := New(context.Background(), pool)
	err := cron.AddJob("this will not parse", nil)
	if err == nil {
		t.Errorf("expected an error with invalid spec, got nil")
	}
}

// Test blocking run method behaves as Start()
func TestBlockingRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	var unblockChan = make(chan struct{})

	go func() {
		cron.Run()
		close(unblockChan)
	}()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job fires")
	case <-unblockChan:
		t.Error("expected that Run() blocks")
	case <-wait(wg):
	}
}

// Test that double-running is a no-op
func TestStartNoop(t *testing.T) {
	var tickChan = make(chan struct{}, 2)

	cron := New(context.Background(), pool)
	cron.AddFunc("* * * * * ?", func() {
		tickChan <- struct{}{}
	})

	cron.Start()
	defer cron.Stop()

	// Wait for the first firing to ensure the runner is going
	<-tickChan

	cron.Start()

	<-tickChan

	// Fail if this job fires again in a short period, indicating a double-run
	select {
	case <-time.After(time.Millisecond):
	case <-tickChan:
		t.Error("expected job fires exactly twice")
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New(context.Background(), pool)
	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"})
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"})
	cron.AddJob("* * * * * ?", testJob{wg, "job2"})
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"})
	cron.schedule(every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"}, "*/5 * * * * *")
	cron.schedule(every(5*time.Minute), testJob{wg, "job5"}, "00 */5 * * * *")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.(testJob).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Fatalf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
		}
	}
}

type ZeroSchedule struct{}

func (*ZeroSchedule) Next(time.Time) time.Time {
	return time.Time{}
}

// Tests that job without time does not run
func TestJobWithZeroTimeDoesNotRun(t *testing.T) {
	cron := New(context.Background(), pool)
	calls := 0
	cron.AddFunc("* * * * * *", func() { calls += 1 })
	cron.schedule(new(ZeroSchedule), FuncJob(func() { t.Error("expected zero task will not run") }), "1 * * * * *")
	cron.Start()
	defer cron.Stop()
	<-time.After(OneSecond)
	if calls != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}
