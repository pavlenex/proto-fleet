package sv2translator

import "time"

type Config struct {
	Enabled       bool          `help:"Route Stratum V2 pool assignments through the bundled SV1-to-SV2 Translator" default:"false" env:"ENABLED"`
	ConfigDir     string        `help:"Shared directory used to publish Translator route configs" default:"/var/lib/fleet/sv2-translator" env:"CONFIG_DIR"`
	AdvertiseHost string        `help:"Host or IP miners use to reach the Translator; auto-detected when empty" default:"" env:"ADVERTISE_HOST"`
	ConnectHost   string        `help:"Host Fleet uses for Translator readiness checks" default:"127.0.0.1" env:"CONNECT_HOST"`
	StartTimeout  time.Duration `help:"Maximum time to wait for a Translator route to accept SV1 connections" default:"20s" env:"START_TIMEOUT"`
	PollInterval  time.Duration `help:"Interval between Translator readiness checks" default:"250ms" env:"POLL_INTERVAL"`
}
