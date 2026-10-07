package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/handlers"
)

// chaosMonkey disrupts one worker container at a time: either SIGKILL (a
// crash: its jobs come back via lease expiry and the reaper) or a graceful
// stop (SIGTERM: in-flight jobs finish or are released). The container is
// started again before the next disruption, so the pool never fully dies.
type chaosMonkey struct {
	project, service string
	every            time.Duration

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	counts chaosActions
}

type chaosActions struct{ kills, stops int }

func newChaosMonkey(project, service string, every time.Duration) *chaosMonkey {
	return &chaosMonkey{project: project, service: service, every: every, done: make(chan struct{})}
}

func (c *chaosMonkey) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	go func() {
		defer close(c.done)
		for {
			// Jitter the interval between 0.5x and 1.5x.
			wait := time.Duration(float64(c.every) * (0.5 + rand.Float64()))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			c.disrupt(ctx)
		}
	}()
}

// stop ends the disruptions and makes sure every container runs again.
func (c *chaosMonkey) stop() chaosActions {
	c.cancel()
	<-c.done
	ids, _ := c.containers(context.Background(), true)
	for _, id := range ids {
		_ = docker(context.Background(), "start", id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts
}

func (c *chaosMonkey) disrupt(ctx context.Context) {
	ids, err := c.containers(ctx, false)
	if err != nil || len(ids) < 2 {
		return // keep at least one worker alive
	}
	id := ids[rand.IntN(len(ids))]
	short := id[:min(12, len(id))]

	kill := rand.IntN(2) == 0
	if kill {
		fmt.Printf("  chaos: SIGKILL %s\n", short)
		_ = docker(ctx, "kill", "--signal", "KILL", id)
	} else {
		fmt.Printf("  chaos: graceful stop %s\n", short)
		_ = docker(ctx, "stop", "--time", "30", id)
	}
	c.mu.Lock()
	if kill {
		c.counts.kills++
	} else {
		c.counts.stops++
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(1000+rand.IntN(2000)) * time.Millisecond):
	}
	fmt.Printf("  chaos: start %s\n", short)
	_ = docker(context.Background(), "start", id)
}

func (c *chaosMonkey) containers(ctx context.Context, all bool) ([]string, error) {
	args := []string{"ps", "-q",
		"--filter", "label=com.docker.compose.project=" + c.project,
		"--filter", "label=com.docker.compose.service=" + c.service}
	if all {
		args = append(args, "-a")
	}
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(out)), nil
}

type chaosResult struct {
	actions       chaosActions
	executions    int // completed handler executions logged for this run
	duplicated    int // jobs executed more than once
	maxPerJob     int
	neverExecuted int // jobs with no logged execution (would mean a lost job)
}

// verify counts "bench job executed" log lines per job id across every
// worker container. Logs of killed containers survive restarts, so an
// execution whose result was lost to a SIGKILL still shows up.
func (c *chaosMonkey) verify(ctx context.Context, runID string, total int) (chaosResult, error) {
	ids, err := c.containers(ctx, true)
	if err != nil {
		return chaosResult{}, err
	}
	perJob := map[int64]int{}
	var res chaosResult
	for _, id := range ids {
		out, err := exec.CommandContext(ctx, "docker", "logs", id).CombinedOutput()
		if err != nil {
			return res, fmt.Errorf("docker logs %s: %w", id, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(out))
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			var line struct {
				Msg   string `json:"msg"`
				Run   string `json:"run"`
				JobID int64  `json:"job_id"`
			}
			if json.Unmarshal(sc.Bytes(), &line) != nil || line.Msg != handlers.BenchExecutedMsg || line.Run != runID {
				continue
			}
			perJob[line.JobID]++
			res.executions++
		}
	}
	for _, n := range perJob {
		if n > 1 {
			res.duplicated++
		}
		res.maxPerJob = max(res.maxPerJob, n)
	}
	res.neverExecuted = total - len(perJob)
	return res, nil
}

func docker(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
