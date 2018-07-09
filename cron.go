package cron

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"reflect"
	"runtime"
	"sort"
	"time"

	"strings"

	"github.com/garyburd/redigo/redis"
)

type Logger interface {
	Printf(format string, v ...interface{})
}

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	stop      chan struct{}
	add       chan *Entry
	snapshot  chan []*Entry
	running   bool
	ErrorLog  Logger
	location  *time.Location
	pool      *redis.Pool
	keyPrefix string
}

const DefaultKeyPrefix = "redis-cron"

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
	Name() string
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	// Example: "* 00 * * * *"
	Spec string
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, in the Local time zone.
func New(pool *redis.Pool) *Cron {
	return NewWithLocation(time.Now().Location(), pool)
}

// NewWithLocation returns a new Cron job runner.
func NewWithLocation(location *time.Location, pool *redis.Pool) *Cron {
	return &Cron{
		entries:   nil,
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan []*Entry),
		running:   false,
		ErrorLog:  nil,
		location:  location,
		pool:      pool,
		keyPrefix: DefaultKeyPrefix,
	}
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

func (f FuncJob) Name() string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// SetKeyPrefix prepend a prefix to the key.
func (c *Cron) SetKeyPrefix(prefix string) {
	c.keyPrefix = prefix
}

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func()) error {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job) error {
	schedule, err := Parse(spec)
	if err != nil {
		return err
	}
	c.schedule(schedule, cmd, spec)
	return nil
}

// schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) schedule(schedule Schedule, cmd Job, spec string) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		Spec:     spec,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() {
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	if c.running {
		return
	}
	c.running = true
	c.run()
}

func (c *Cron) lock(start time.Time, next time.Time, entry *Entry) (bool, error) {
	key := c.constructKey(entry)
	expireAfter := c.calcExpiry(start.In(c.location), next.In(c.location))
	conn := c.pool.Get()
	defer conn.Close()
	reply, err := redis.String(conn.Do("SET", key, 1, "NX", "PX", int(expireAfter)))
	return reply == "OK" && err == nil, err
}

// in millisecond
func (c *Cron) calcExpiry(now time.Time, next time.Time) int64 {
	nowUnix := now.UnixNano()
	nextUnix := next.UnixNano()
	expiry := ((nextUnix - nowUnix) / 2) / 1e6
	return expiry
}

func (c *Cron) constructKey(entry *Entry) string {
	h := sha1.New()
	h.Write([]byte(c.keyPrefix + entry.Job.Name() + entry.Spec))
	key := hex.EncodeToString(h.Sum(nil))
	return key
}

func (c *Cron) runWithRecovery(e *Entry, start time.Time, next time.Time) {
	defer func() {
		if r := recover(); r != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			c.logf("cron: panic running job: %v\n%s", r, buf)
		}
	}()
	locked, err := c.lock(start, next, e)
	if err != nil || !locked {
		if !strings.Contains(err.Error(), "nil returned") {
			panic(err)
		}
		return
	}
	e.Job.Run()
}

// Run the scheduler. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
					go c.runWithRecovery(e, now, e.Next)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)

			case <-c.snapshot:
				c.snapshot <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				return
			}

			break
		}

		timer.Stop()
	}
}

// Logs an error to stderr or to the configured error log
func (c *Cron) logf(format string, args ...interface{}) {
	if c.ErrorLog != nil {
		c.ErrorLog.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
		})
	}
	return entries
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}
