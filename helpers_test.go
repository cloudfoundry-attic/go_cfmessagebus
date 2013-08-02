package cfmessagebus

import (
	"errors"
	"fmt"
	. "launchpad.net/gocheck"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

func StartNats(port int) *exec.Cmd {
	cmd := exec.Command("nats-server", "-p", strconv.Itoa(port), "--user", "nats", "--pass", "nats")
	err := cmd.Start()
	if err != nil {
		panic(fmt.Sprintf("NATS failed to start: %v\n", err))
	}

	return cmd
}

func StopNats(cmd *exec.Cmd) {
	cmd.Process.Kill()
	cmd.Wait()
}

func waitUntilNatsUp(port int) error {
	maxWait := 10
	for i := 0; i < maxWait; i++ {
		time.Sleep(500 * time.Millisecond)
		_, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			return nil
		}
	}

	return errors.New("Waited too long for NATS to start")
}

func waitUntilNatsDown(port int) error {
	maxWait := 10
	for i := 0; i < maxWait; i++ {
		time.Sleep(500 * time.Millisecond)
		_, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return nil
		}
	}

	return errors.New("Waited too long for NATS to stop")
}

func failOnTimeout(successChan chan bool, timeout time.Duration, onSuccess func()) {
	onTimeout := func() { panic("Timed out") }
	withTimeout(successChan, timeout, onSuccess, onTimeout)
}

func failOnEvent(eventChan chan bool, timeout time.Duration, onTimeout func()) {
	onEvent := func() { panic("Event should not have happened") }
	withTimeout(eventChan, timeout, onEvent, onTimeout)
}

func withTimeout(successChan chan bool, timeout time.Duration, onSuccess func(), onTimeout func()) {
	timeoutChan := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutChan <- true
	}()

	select {
	case <-successChan:
		onSuccess()
	case <-timeoutChan:
		onTimeout()
	}
}
