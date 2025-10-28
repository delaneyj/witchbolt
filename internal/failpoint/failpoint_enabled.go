//go:build failpoint

package failpoint

import (
	"fmt"
	"sync"
	"time"
)

var (
	mu         sync.RWMutex
	failpoints = make(map[string]*Failpoint)
)

type Failpoint struct {
	action string
}

// Enable enables a failpoint with the given action.
// Supported actions:
//   - return("error message") - returns an error
//   - sleep(milliseconds) - sleeps for the given duration
func Enable(name string, action string) error {
	mu.Lock()
	defer mu.Unlock()
	failpoints[name] = &Failpoint{action: action}
	return nil
}

// Disable disables a failpoint.
func Disable(name string) error {
	mu.Lock()
	defer mu.Unlock()
	delete(failpoints, name)
	return nil
}

// Inject checks if a failpoint is enabled and executes its action.
// Returns an error message if the failpoint should return an error.
func Inject(name string) (string, bool) {
	mu.RLock()
	fp := failpoints[name]
	mu.RUnlock()

	if fp == nil {
		return "", false
	}

	// Parse action
	if len(fp.action) > 8 && fp.action[:7] == "return(" && fp.action[len(fp.action)-1] == ')' {
		msg := fp.action[8 : len(fp.action)-2] // remove return(" and ")
		return msg, true
	}

	if len(fp.action) > 6 && fp.action[:6] == "sleep(" && fp.action[len(fp.action)-1] == ')' {
		// Parse sleep duration
		var ms int
		fmt.Sscanf(fp.action[6:len(fp.action)-1], "%d", &ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return "", false
	}

	return "", false
}

// InjectStruct checks if a struct-type failpoint is enabled and executes its action.
// This is for failpoints that just need to trigger an action without returning an error.
func InjectStruct(name string) {
	mu.RLock()
	fp := failpoints[name]
	mu.RUnlock()

	if fp == nil {
		return
	}

	// Parse action (same as Inject but doesn't return anything)
	if len(fp.action) > 6 && fp.action[:6] == "sleep(" && fp.action[len(fp.action)-1] == ')' {
		var ms int
		fmt.Sscanf(fp.action[6:len(fp.action)-1], "%d", &ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
