package sv2translator

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/block/proto-fleet/server/internal/domain/sv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAuthorityKey = "9awtMD5KQgvRUh2yFbjVeT7b6hjipWcAsQHd6wEhgtDT9soosna"

type stubRouteStore struct {
	route Route
	err   error
}

func (s stubRouteStore) GetOrCreate(context.Context, int64, string, string) (Route, error) {
	return s.route, s.err
}

func (s stubRouteStore) GetByPort(context.Context, int64, int32) (Route, error) {
	return s.route, s.err
}

func TestRenderConfigMatchesReleasedTranslatorSchema(t *testing.T) {
	config, err := renderConfig(Route{
		OrganizationID: 7,
		UpstreamURL:    "stratum2+tcp://v2.example.com:3336/" + testAuthorityKey,
		Username:       `wallet"name`,
		ListenPort:     34255,
	})

	require.NoError(t, err)
	text := string(config)
	assert.Contains(t, text, "downstream_port = 34255")
	assert.Contains(t, text, `address = "v2.example.com"`)
	assert.Contains(t, text, "port = 3336")
	assert.Contains(t, text, `authority_pubkey = "`+testAuthorityKey+`"`)
	assert.Contains(t, text, `user_identity = "wallet\"name"`)
	assert.Contains(t, text, "supported_extensions = [0x0002]")
	assert.NotContains(t, text, "monitoring_address")
}

func TestRenderConfigNormalizesHexAuthorityKey(t *testing.T) {
	authorityKey, err := sv2.PoolNoiseKeyFromURL(
		"stratum2+tcp://v2.example.com:3336/" + testAuthorityKey,
	)
	require.NoError(t, err)

	config, err := renderConfig(Route{
		UpstreamURL: "stratum2+tcp://v2.example.com:3336/" +
			hex.EncodeToString(authorityKey),
		Username:   "wallet",
		ListenPort: 34256,
	})

	require.NoError(t, err)
	assert.Contains(t, string(config), `authority_pubkey = "`+testAuthorityKey+`"`)
}

func TestRoutePublishesConfigAndReturnsReadySV1Listener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	tcpAddress, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	// A TCP port is bounded to 16 bits, so this conversion cannot overflow.
	listenPort := int32(tcpAddress.Port) //nolint:gosec
	route := Route{
		OrganizationID: 7,
		UpstreamURL:    "stratum2+tcp://v2.example.com:3336/" + testAuthorityKey,
		Username:       "wallet",
		ListenPort:     listenPort,
	}
	config, err := renderConfig(route)
	require.NoError(t, err)

	configDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(configDir, fmt.Sprintf("route-%d.ready", listenPort)),
		[]byte(configChecksum(config)),
		0o644,
	))
	manager := &Manager{
		cfg: Config{
			ConfigDir:    configDir,
			ConnectHost:  "127.0.0.1",
			StartTimeout: time.Second,
			PollInterval: 10 * time.Millisecond,
		},
		routes:        stubRouteStore{route: route},
		advertiseHost: "192.168.1.10",
	}

	localURL, localUsername, localPassword, err := manager.Route(
		context.Background(),
		7,
		route.UpstreamURL,
		route.Username,
	)

	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("stratum+tcp://192.168.1.10:%d", listenPort), localURL)
	assert.Equal(t, "wallet", localUsername)
	assert.Equal(t, "x", localPassword)
	published, err := os.ReadFile(filepath.Join(configDir, fmt.Sprintf("route-%d.toml", listenPort)))
	require.NoError(t, err)
	assert.Equal(t, config, published)
}

func TestResolveMapsAdvertisedListenerToUpstream(t *testing.T) {
	store := stubRouteStore{route: Route{
		OrganizationID: 7,
		UpstreamURL:    "stratum2+tcp://v2.example.com:3336/" + testAuthorityKey,
		Username:       "wallet",
		ListenPort:     34255,
	}}
	manager := &Manager{
		routes:        store,
		advertiseHost: "192.168.1.10",
	}

	upstreamURL, routeUsername, ok, err := manager.Resolve(
		context.Background(),
		7,
		"stratum+tcp://192.168.1.10:34255",
	)

	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, store.route.UpstreamURL, upstreamURL)
	assert.Equal(t, "wallet", routeUsername)
}

func TestResolveIgnoresOtherHostsAndUnknownPorts(t *testing.T) {
	manager := &Manager{
		routes:        stubRouteStore{err: sql.ErrNoRows},
		advertiseHost: "192.168.1.10",
	}

	_, _, ok, err := manager.Resolve(context.Background(), 7, "stratum+tcp://192.168.1.11:34255")
	require.NoError(t, err)
	assert.False(t, ok)

	_, _, ok, err = manager.Resolve(context.Background(), 7, "stratum+tcp://192.168.1.10:34255")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestNewManagerRejectsInvalidConfig(t *testing.T) {
	_, err := NewManager(Config{}, stubRouteStore{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config directory")

	_, err = NewManager(Config{
		ConfigDir:     t.TempDir(),
		AdvertiseHost: "bad/host",
		ConnectHost:   "127.0.0.1",
		StartTimeout:  1,
		PollInterval:  1,
	}, stubRouteStore{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("%q is invalid", "bad/host"))
}

func TestNewManagerNormalizesBracketedIPv6Hosts(t *testing.T) {
	manager, err := NewManager(Config{
		ConfigDir:     t.TempDir(),
		AdvertiseHost: "[2001:db8::10]",
		ConnectHost:   "[::1]",
		StartTimeout:  1,
		PollInterval:  1,
	}, stubRouteStore{})

	require.NoError(t, err)
	assert.Equal(t, "2001:db8::10", manager.advertiseHost)
	assert.Equal(t, "::1", manager.cfg.ConnectHost)
}
