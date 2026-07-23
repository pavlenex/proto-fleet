package translator

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const managerTestURL = "stratum2+tcp://pool.example.com:34254/9bXiEd8boQVhq7WddEcERUL5tyyJVFYdU8th3HfbNXK3Yw6GRXh"

type managerTestRuntime struct {
	startCalls int
	stopCalls  int
}

func (f *managerTestRuntime) EnsureStarted(context.Context) error {
	f.startCalls++
	return nil
}

func (f *managerTestRuntime) State(context.Context) (runtimeState, error) {
	return runtimeState{Exists: true, Running: true, Managed: true, Image: Image}, nil
}

func (f *managerTestRuntime) Stop(context.Context) error {
	f.stopCalls++
	return nil
}

func TestEnsureProfilePersistsAndStartsFixedProfile(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	port := listener.Addr().(*net.TCPAddr).Port

	manager, err := NewManager(Config{
		StateDir:       t.TempDir(),
		AdvertisedHost: "10.0.0.5",
		ConnectHost:    "127.0.0.1",
		DownstreamPort: uint16(port), // #nosec G115 -- ephemeral TCP ports fit in uint16.
		ReadyTimeout:   time.Second,
		DockerSocket:   "/var/run/docker.sock",
	})
	require.NoError(t, err)
	runtime := &managerTestRuntime{}
	manager.runtime = runtime
	profile := Profile{Upstreams: []Upstream{{URL: managerTestURL, Username: "account"}}}

	endpoint, err := manager.EnsureProfile(context.Background(), profile)

	require.NoError(t, err)
	assert.Equal(t, "stratum+tcp://10.0.0.5:"+strconv.Itoa(port), endpoint.String())
	assert.Equal(t, 1, runtime.startCalls)
	_, err = os.Stat(filepath.Join(manager.config.StateDir, configFileName))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(manager.config.StateDir, profileFileName))
	require.NoError(t, err)

	_, err = manager.EnsureProfile(context.Background(), Profile{
		Upstreams: []Upstream{{
			URL:      "stratum2+tcp://other.example.com:34254/9bXiEd8boQVhq7WddEcERUL5tyyJVFYdU8th3HfbNXK3Yw6GRXh",
			Username: "other",
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different pool set")
	assert.Equal(t, 1, runtime.startCalls)
}

func TestRenderConfigUsesCanonicalAuthorityKeyAndPerUpstreamIdentity(t *testing.T) {
	rendered, err := renderConfig(
		Profile{Upstreams: []Upstream{{
			URL:      managerTestURL,
			Username: "account.worker",
		}}},
		34255,
	)

	require.NoError(t, err)
	text := string(rendered)
	assert.Contains(t, text, "downstream_port = 34255")
	assert.Contains(t, text, `authority_pubkey = "9bXiEd8boQVhq7WddEcERUL5tyyJVFYdU8th3HfbNXK3Yw6GRXh"`)
	assert.Contains(t, text, `user_identity = "account.worker"`)
}
