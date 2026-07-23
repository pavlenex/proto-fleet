package sv2translator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/proto-fleet/server/internal/domain/sv2"
	"github.com/block/proto-fleet/server/internal/infrastructure/networking"
)

const localPoolPassword = "x"

type Manager struct {
	cfg           Config
	routes        RouteStore
	advertiseHost string
	mu            sync.Mutex
}

func NewManager(cfg Config, routes RouteStore) (*Manager, error) {
	if routes == nil {
		return nil, errors.New("SV2 Translator route store is required")
	}
	if strings.TrimSpace(cfg.ConfigDir) == "" {
		return nil, errors.New("SV2 Translator config directory is required")
	}
	if cfg.StartTimeout <= 0 {
		return nil, errors.New("SV2 Translator start timeout must be positive")
	}
	if cfg.PollInterval <= 0 {
		return nil, errors.New("SV2 Translator poll interval must be positive")
	}

	advertiseHost := strings.TrimSpace(cfg.AdvertiseHost)
	if advertiseHost == "" {
		info, err := networking.GetLocalNetworkInfo()
		if err != nil {
			return nil, fmt.Errorf("auto-detect SV2 Translator advertise host: %w", err)
		}
		advertiseHost = info.LocalIP
		if advertiseHost == "" {
			advertiseHost = info.LocalIPv6
		}
		if advertiseHost == "" {
			return nil, errors.New("auto-detect SV2 Translator advertise host: no usable address")
		}
	}
	advertiseHost, err := normalizeHost("advertise", advertiseHost)
	if err != nil {
		return nil, err
	}
	connectHost, err := normalizeHost("connect", cfg.ConnectHost)
	if err != nil {
		return nil, err
	}
	cfg.ConnectHost = connectHost

	if err := os.MkdirAll(cfg.ConfigDir, 0o750); err != nil {
		return nil, fmt.Errorf("create SV2 Translator config directory: %w", err)
	}

	return &Manager{
		cfg:           cfg,
		routes:        routes,
		advertiseHost: advertiseHost,
	}, nil
}

// Route ensures a dedicated Translator process is ready, then returns the
// local SV1 pool settings that should be sent to the miner.
func (m *Manager) Route(
	ctx context.Context,
	organizationID int64,
	upstreamURL string,
	username string,
) (localURL string, localUsername string, localPassword string, err error) {
	if !sv2.IsSV2URL(upstreamURL) {
		return upstreamURL, username, "", nil
	}
	if err := sv2.ValidatePoolURL(upstreamURL); err != nil {
		return "", "", "", err
	}

	route, err := m.routes.GetOrCreate(ctx, organizationID, upstreamURL, username)
	if err != nil {
		return "", "", "", fmt.Errorf("allocate Translator route: %w", err)
	}
	config, err := renderConfig(route)
	if err != nil {
		return "", "", "", err
	}

	// Serialize publication and readiness checks so two assignments cannot
	// race a config replacement for the same stable listener.
	m.mu.Lock()
	defer m.mu.Unlock()

	checksum := configChecksum(config)
	if err := m.publishConfig(route.ListenPort, config, checksum); err != nil {
		return "", "", "", err
	}
	if err := m.waitUntilReady(ctx, route.ListenPort, checksum); err != nil {
		return "", "", "", err
	}

	return "stratum+tcp://" + net.JoinHostPort(m.advertiseHost, strconv.Itoa(int(route.ListenPort))),
		username,
		localPoolPassword,
		nil
}

// Resolve maps a miner-reported local proxy URL back to the upstream pool so
// the assignment UI continues to recognize saved SV2 pools.
func (m *Manager) Resolve(
	ctx context.Context,
	organizationID int64,
	configuredURL string,
) (upstreamURL string, routeUsername string, ok bool, err error) {
	u, err := url.Parse(configuredURL)
	if err != nil || !strings.EqualFold(u.Scheme, "stratum+tcp") {
		return "", "", false, nil //nolint:nilerr // malformed non-route URLs retain existing display behavior
	}
	if !sameHost(u.Hostname(), m.advertiseHost) {
		return "", "", false, nil
	}
	port, err := strconv.ParseInt(u.Port(), 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return "", "", false, nil //nolint:nilerr // non-numeric ports cannot identify a Translator route
	}

	route, err := m.routes.GetByPort(ctx, organizationID, int32(port))
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("resolve Translator route: %w", err)
	}
	return route.UpstreamURL, route.Username, true, nil
}

func renderConfig(route Route) ([]byte, error) {
	u, err := url.Parse(route.UpstreamURL)
	if err != nil {
		return nil, fmt.Errorf("parse SV2 pool URL: %w", err)
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("SV2 pool URL %q has an invalid port", route.UpstreamURL)
	}
	authorityKey, err := sv2.CanonicalAuthorityPublicKeyFromURL(route.UpstreamURL)
	if err != nil {
		return nil, fmt.Errorf("normalize SV2 authority key: %w", err)
	}
	address, err := tomlString(u.Hostname())
	if err != nil {
		return nil, err
	}
	authority, err := tomlString(authorityKey)
	if err != nil {
		return nil, err
	}
	identity, err := tomlString(route.Username)
	if err != nil {
		return nil, err
	}

	config := fmt.Sprintf(`# Generated by Proto Fleet. Managed automatically.
downstream_address = "0.0.0.0"
downstream_port = %d
max_supported_version = 2
min_supported_version = 2
downstream_extranonce2_size = 4
verify_payout = false
aggregate_channels = true
supported_extensions = [0x0002]
required_extensions = []

[downstream_difficulty_config]
min_individual_miner_hashrate = 10_000_000_000_000.0
shares_per_minute = 6.0
enable_vardiff = true
job_keepalive_interval_secs = 60

[[upstreams]]
address = %s
port = %d
authority_pubkey = %s
user_identity = %s
`, route.ListenPort, address, port, authority, identity)
	return []byte(config), nil
}

func tomlString(value string) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode Translator config string: %w", err)
	}
	return string(encoded), nil
}

func configChecksum(config []byte) string {
	sum := sha256.Sum256(config)
	return hex.EncodeToString(sum[:])
}

func (m *Manager) publishConfig(listenPort int32, config []byte, checksum string) error {
	configPath := m.configPath(listenPort)
	existing, err := os.ReadFile(configPath)
	if err == nil && configChecksum(existing) == checksum {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read Translator config: %w", err)
	}

	tmp, err := os.CreateTemp(m.cfg.ConfigDir, ".route-*.toml")
	if err != nil {
		return fmt.Errorf("create temporary Translator config: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set Translator config permissions: %w", err)
	}
	if _, err := tmp.Write(config); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write Translator config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync Translator config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close Translator config: %w", err)
	}
	if err := os.Rename(tmpPath, configPath); err != nil {
		return fmt.Errorf("publish Translator config: %w", err)
	}
	return nil
}

func (m *Manager) waitUntilReady(ctx context.Context, listenPort int32, checksum string) error {
	waitCtx, cancel := context.WithTimeout(ctx, m.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	for {
		ready, err := os.ReadFile(m.readyPath(listenPort))
		if err == nil && strings.TrimSpace(string(ready)) == checksum {
			address := net.JoinHostPort(m.cfg.ConnectHost, strconv.Itoa(int(listenPort)))
			conn, dialErr := (&net.Dialer{Timeout: m.cfg.PollInterval}).DialContext(waitCtx, "tcp", address)
			if dialErr == nil {
				_ = conn.Close()
				return nil
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read Translator readiness marker: %w", err)
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf(
				"Translator route on port %d did not become ready within %s: %w",
				listenPort,
				m.cfg.StartTimeout,
				waitCtx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func (m *Manager) configPath(listenPort int32) string {
	return filepath.Join(m.cfg.ConfigDir, fmt.Sprintf("route-%d.toml", listenPort))
}

func (m *Manager) readyPath(listenPort int32) string {
	return filepath.Join(m.cfg.ConfigDir, fmt.Sprintf("route-%d.ready", listenPort))
}

func sameHost(left, right string) bool {
	left = strings.Trim(strings.TrimSpace(left), "[]")
	right = strings.Trim(strings.TrimSpace(right), "[]")
	leftIP := net.ParseIP(left)
	rightIP := net.ParseIP(right)
	if leftIP != nil || rightIP != nil {
		return leftIP != nil && rightIP != nil && leftIP.Equal(rightIP)
	}
	return strings.EqualFold(left, right)
}

func normalizeHost(label, raw string) (string, error) {
	host := strings.TrimSpace(raw)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	if host == "" {
		return "", fmt.Errorf("SV2 Translator %s host is required", label)
	}
	if strings.ContainsAny(host, "/?#[] \t\r\n") ||
		(strings.Contains(host, ":") && net.ParseIP(host) == nil) {
		return "", fmt.Errorf("SV2 Translator %s host %q is invalid", label, raw)
	}
	return host, nil
}
