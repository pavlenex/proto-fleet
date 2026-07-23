package translator

import (
	"context"
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
)

const (
	configFileName  = "tproxy.toml"
	profileFileName = "active-profile.json"
	profileVersion  = 1
)

type Config struct {
	StateDir       string        `help:"Directory shared with the pre-created SV2 translator container" default:"/var/lib/proto-fleet/sv2" env:"STATE_DIR"`
	AdvertisedHost string        `help:"Private listener IP sent to SV1 miners; empty selects the route to the assigned SV2 pool" default:"" env:"ADVERTISED_HOST"`
	ConnectHost    string        `help:"Listener host Fleet probes for readiness; empty uses the advertised host" default:"" env:"CONNECT_HOST"`
	DownstreamPort uint16        `help:"SV1 listener port exposed by the translator" default:"34255" env:"DOWNSTREAM_PORT"`
	ReadyTimeout   time.Duration `help:"Maximum time to start the pre-created translator and verify its listener" default:"30s" env:"READY_TIMEOUT"`
	DockerSocket   string        `help:"Docker Engine socket used only to inspect/start/stop the fixed translator container" default:"/var/run/docker.sock" env:"DOCKER_SOCKET"`
}

type Manager interface {
	EnsureProfile(context.Context, Profile) (Endpoint, error)
	ActiveProfile() (Profile, Endpoint, bool)
}

type persistedProfile struct {
	Version  int      `json:"version"`
	Profile  Profile  `json:"profile"`
	Endpoint Endpoint `json:"endpoint"`
}

type FileManager struct {
	config  Config
	runtime containerRuntime

	mu       sync.Mutex
	active   bool
	profile  Profile
	endpoint Endpoint
}

var _ Manager = (*FileManager)(nil)

func NewManager(config Config) (*FileManager, error) {
	if !filepath.IsAbs(config.StateDir) {
		return nil, fmt.Errorf("SV2 translator state directory must be absolute")
	}
	if config.DownstreamPort == 0 {
		return nil, fmt.Errorf("SV2 translator downstream port must be non-zero")
	}
	if config.ReadyTimeout <= 0 {
		return nil, fmt.Errorf("SV2 translator ready timeout must be positive")
	}
	if !filepath.IsAbs(config.DockerSocket) {
		return nil, fmt.Errorf("SV2 translator Docker socket must be absolute")
	}
	if config.AdvertisedHost != "" {
		if _, err := privateIP("advertised", config.AdvertisedHost, false); err != nil {
			return nil, err
		}
	}
	if config.ConnectHost != "" {
		connectHost := strings.Trim(strings.TrimSpace(config.ConnectHost), "[]")
		if connectHost == "" || strings.ContainsAny(connectHost, "/\\") {
			return nil, fmt.Errorf("SV2 translator readiness host %q is invalid", config.ConnectHost)
		}
	}

	manager := &FileManager{
		config:  config,
		runtime: newDockerRuntime(config.DockerSocket),
	}
	if err := manager.loadActiveProfile(); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *FileManager) EnsureProfile(ctx context.Context, desired Profile) (Endpoint, error) {
	normalized, err := NormalizeProfile(desired)
	if err != nil {
		return "", err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	firstActivation := !m.active
	if m.active && !ProfilesEqual(m.profile, normalized) {
		return "", fmt.Errorf(
			"SV2 translation already serves a different pool set; reassign existing translated miners before changing it",
		)
	}

	endpoint := m.endpoint
	if firstActivation {
		endpoint, err = m.endpointForProfile(ctx, normalized)
		if err != nil {
			return "", err
		}
	}

	rendered, err := renderConfig(normalized, m.config.DownstreamPort)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(m.config.StateDir, 0o700); err != nil {
		return "", fmt.Errorf("create SV2 translator state directory: %w", err)
	}
	if err := atomicWrite(m.configPath(), rendered, 0o600); err != nil {
		return "", err
	}
	if firstActivation {
		record, marshalErr := json.Marshal(persistedProfile{
			Version:  profileVersion,
			Profile:  normalized,
			Endpoint: endpoint,
		})
		if marshalErr != nil {
			_ = os.Remove(m.configPath())
			return "", fmt.Errorf("encode SV2 translator profile: %w", marshalErr)
		}
		if err := atomicWrite(m.profilePath(), record, 0o600); err != nil {
			_ = os.Remove(m.configPath())
			return "", err
		}
	}

	startupCtx, cancel := context.WithTimeout(ctx, m.config.ReadyTimeout)
	defer cancel()
	if err := m.runtime.EnsureStarted(startupCtx); err != nil {
		if firstActivation {
			m.rollbackFirstActivation()
		}
		return "", err
	}
	if err := m.waitReady(startupCtx, endpoint); err != nil {
		if firstActivation {
			m.rollbackFirstActivation()
		}
		return "", err
	}

	if firstActivation {
		m.active = true
		m.profile = cloneProfile(normalized)
		m.endpoint = endpoint
	}
	return endpoint, nil
}

func (m *FileManager) ActiveProfile() (Profile, Endpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.active {
		return Profile{}, "", false
	}
	return cloneProfile(m.profile), m.endpoint, true
}

func (m *FileManager) loadActiveProfile() error {
	data, err := os.ReadFile(m.profilePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read persisted SV2 translator profile: %w", err)
	}
	if len(data) > 64*1024 {
		return fmt.Errorf("persisted SV2 translator profile is too large")
	}

	var persisted persistedProfile
	if err := json.Unmarshal(data, &persisted); err != nil {
		return fmt.Errorf("decode persisted SV2 translator profile: %w", err)
	}
	if persisted.Version != profileVersion {
		return fmt.Errorf("unsupported persisted SV2 translator profile version %d", persisted.Version)
	}
	normalized, err := NormalizeProfile(persisted.Profile)
	if err != nil {
		return fmt.Errorf("validate persisted SV2 translator profile: %w", err)
	}
	if !ProfilesEqual(normalized, persisted.Profile) {
		return fmt.Errorf("persisted SV2 translator profile is not normalized")
	}
	if err := validateEndpoint(persisted.Endpoint, m.config.DownstreamPort); err != nil {
		return fmt.Errorf("validate persisted SV2 translator endpoint: %w", err)
	}

	m.active = true
	m.profile = cloneProfile(normalized)
	m.endpoint = persisted.Endpoint
	return nil
}

func (m *FileManager) endpointForProfile(ctx context.Context, profile Profile) (Endpoint, error) {
	host := m.config.AdvertisedHost
	if host == "" {
		selected, err := routeIP(ctx, profile.Upstreams[0].URL)
		if err != nil {
			return "", err
		}
		host = selected.String()
	}
	ip, err := privateIP("advertised", host, false)
	if err != nil {
		return "", err
	}
	return Endpoint("stratum+tcp://" + net.JoinHostPort(ip.String(), strconv.Itoa(int(m.config.DownstreamPort)))), nil
}

func (m *FileManager) waitReady(ctx context.Context, endpoint Endpoint) error {
	connectHost := m.config.ConnectHost
	if connectHost == "" {
		parsed, err := url.Parse(endpoint.String())
		if err != nil {
			return fmt.Errorf("parse SV2 translator endpoint: %w", err)
		}
		connectHost = parsed.Hostname()
	}
	address := net.JoinHostPort(connectHost, strconv.Itoa(int(m.config.DownstreamPort)))
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		conn, err := (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext(ctx, "tcp", address)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		state, stateErr := m.runtime.State(ctx)
		if stateErr == nil && state.Exists && !state.Running {
			return fmt.Errorf("SV2 translator stopped before its listener became ready: %s", state.Detail)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for SV2 translator listener %s: %w", address, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m *FileManager) rollbackFirstActivation() {
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = m.runtime.Stop(stopCtx)
	_ = os.Remove(m.profilePath())
	_ = os.Remove(m.configPath())
}

func (m *FileManager) configPath() string {
	return filepath.Join(m.config.StateDir, configFileName)
}

func (m *FileManager) profilePath() string {
	return filepath.Join(m.config.StateDir, profileFileName)
}

func renderConfig(profile Profile, downstreamPort uint16) ([]byte, error) {
	var upstreams strings.Builder
	for _, upstream := range profile.Upstreams {
		parsed, err := url.Parse(upstream.URL)
		if err != nil {
			return nil, fmt.Errorf("parse SV2 translator upstream: %w", err)
		}
		port, err := strconv.ParseUint(parsed.Port(), 10, 16)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("SV2 translator upstream %q has an invalid port", upstream.URL)
		}
		authority, err := sv2.CanonicalAuthorityPublicKeyFromURL(upstream.URL)
		if err != nil {
			return nil, fmt.Errorf("normalize SV2 authority key: %w", err)
		}
		addressJSON, _ := json.Marshal(parsed.Hostname())
		authorityJSON, _ := json.Marshal(authority)
		usernameJSON, _ := json.Marshal(upstream.Username)
		fmt.Fprintf(&upstreams, `
[[upstreams]]
address = %s
port = %d
authority_pubkey = %s
user_identity = %s
`, addressJSON, port, authorityJSON, usernameJSON)
	}

	config := fmt.Sprintf(`downstream_address = "0.0.0.0"
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
%s`, downstreamPort, upstreams.String())
	return []byte(config), nil
}

func routeIP(ctx context.Context, upstreamURL string) (net.IP, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, fmt.Errorf("parse SV2 upstream for listener selection: %w", err)
	}
	address := net.JoinHostPort(parsed.Hostname(), parsed.Port())
	conn, err := (&net.Dialer{}).DialContext(ctx, "udp", address)
	if err != nil {
		return nil, fmt.Errorf("select miner-reachable SV2 translator address: %w", err)
	}
	defer conn.Close()
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, fmt.Errorf("select miner-reachable SV2 translator address: unexpected local address")
	}
	return privateIP("automatically selected", local.IP.String(), false)
}

func privateIP(label, raw string, allowLoopback bool) (net.IP, error) {
	ip := net.ParseIP(strings.Trim(strings.TrimSpace(raw), "[]"))
	if ip == nil || ip.IsUnspecified() || (!allowLoopback && ip.IsLoopback()) ||
		(!ip.IsPrivate() && !(allowLoopback && ip.IsLoopback())) {
		return nil, fmt.Errorf("SV2 translator %s host %q must be a private IP", label, raw)
	}
	return ip, nil
}

func validateEndpoint(endpoint Endpoint, expectedPort uint16) error {
	parsed, err := url.Parse(endpoint.String())
	if err != nil {
		return err
	}
	if parsed.Scheme != "stratum+tcp" {
		return fmt.Errorf("unexpected scheme %q", parsed.Scheme)
	}
	if _, err := privateIP("persisted endpoint", parsed.Hostname(), false); err != nil {
		return err
	}
	if parsed.Port() != strconv.Itoa(int(expectedPort)) {
		return fmt.Errorf("unexpected port %q", parsed.Port())
	}
	return nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".sv2-*")
	if err != nil {
		return fmt.Errorf("create temporary SV2 translator state: %w", err)
	}
	tempPath := file.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return fmt.Errorf("set SV2 translator state permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write SV2 translator state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync SV2 translator state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close SV2 translator state: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish SV2 translator state: %w", err)
	}
	return nil
}

func cloneProfile(profile Profile) Profile {
	return Profile{Upstreams: append([]Upstream(nil), profile.Upstreams...)}
}
