package wsshared

import (
	"testing"

	"go.uber.org/goleak"
)

// Every test in the package runs under a goroutine-leak check: a read loop,
// heartbeat or reconnect goroutine left behind fails the run. At 10k VUs one
// leaked goroutine per iteration is a generator that dies an hour in.
func TestMain(m *testing.M) {
	// No blanket ignores on purpose: ignoring internal/poll.runtime_pollWait
	// would also hide a leaked read loop, which blocks exactly there.
	goleak.VerifyTestMain(m)
}
