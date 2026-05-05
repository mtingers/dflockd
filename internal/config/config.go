// Package config loads dflockd configuration from CLI flags and env vars.
//
// Precedence: explicit CLI flag > environment variable > flag default.
package config

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved server configuration.
type Config struct {
	// TCP server.
	Host                    string
	Port                    int
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ShutdownTimeout         time.Duration
	MaxConnections          int
	MaxConnectionsPerIP     int
	AutoReleaseOnDisconnect bool
	TLSCert                 string
	TLSKey                  string
	AuthToken               string

	// Lock manager.
	DefaultLeaseTTL    time.Duration
	LeaseSweepInterval time.Duration
	GCInterval         time.Duration
	GCMaxIdleTime      time.Duration
	MaxLocks           int
	MaxWaiters         int

	// HTTP API (HTTPPort=0 disables).
	HTTPPort                int
	HTTPHost                string
	HTTPSessionIdleTimeout  time.Duration
	HTTPMaxSessions         int
	HTTPMaxSessionsPerIP    int
	HTTPMaxConnectionsPerIP int
	HTTPRateLimitPerIP      int
	HTTPRateLimitBurst      int
	HTTPCORSAllowedOrigins  []string

	// Diagnostics.
	Debug   bool
	Version bool
}

// maxDurationSeconds caps the seconds-as-int conversions. time.Duration
// runs out of range above ~292 years; values beyond that wrap negative.
const maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)

// Load parses args and returns the resolved Config. Returns an error
// (rather than calling os.Exit) so callers can present errors however
// they like.
func Load(args []string) (*Config, error) {
	fs := flag.NewFlagSet("dflockd", flag.ContinueOnError)

	host := fs.String("host", "127.0.0.1", "Bind address")
	port := fs.Int("port", 6388, "Bind port")
	defaultLeaseTTL := fs.Int("default-lease-ttl", 33, "Default lock lease duration (seconds)")
	leaseSweepInterval := fs.Int("lease-sweep-interval", 1, "Lease expiry check interval (seconds)")
	gcInterval := fs.Int("gc-interval", 5, "Lock state GC interval (seconds)")
	gcMaxIdle := fs.Int("gc-max-idle", 60, "Idle seconds before pruning lock state")
	maxLocks := fs.Int("max-locks", 1024, "Maximum number of unique lock keys")
	maxConnections := fs.Int("max-connections", 0, "Maximum concurrent connections (0 = unlimited)")
	maxConnectionsPerIP := fs.Int("max-connections-per-ip", 0, "Maximum concurrent TCP connections per remote IP (0 = unlimited)")
	maxWaiters := fs.Int("max-waiters", 0, "Maximum waiters per lock/semaphore key (0 = unlimited)")
	readTimeout := fs.Int("read-timeout", 23, "Client read timeout (seconds)")
	writeTimeout := fs.Int("write-timeout", 5, "Client write timeout (seconds)")
	shutdownTimeout := fs.Int("shutdown-timeout", 30, "Graceful shutdown drain timeout (seconds, 0 = wait forever)")
	autoRelease := fs.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	tlsCert := fs.String("tls-cert", "", "Path to TLS certificate PEM file")
	tlsKey := fs.String("tls-key", "", "Path to TLS private key PEM file")
	authToken := fs.String("auth-token", "", "Shared secret token (visible in process list; prefer --auth-token-file)")
	authTokenFile := fs.String("auth-token-file", "", "Path to file containing the auth token (one line)")
	httpPort := fs.Int("http-port", 0, "HTTP API listen port (0 = disabled)")
	httpHost := fs.String("http-host", "", "HTTP API bind address (defaults to --host)")
	httpIdle := fs.Int("http-session-idle-timeout", 20, "HTTP session idle timeout (seconds)")
	httpMaxSessions := fs.Int("http-max-sessions", 0, "Max concurrent HTTP sessions (0 = unlimited)")
	httpMaxSessionsPerIP := fs.Int("http-max-sessions-per-ip", 0, "Max concurrent HTTP sessions per remote IP (0 = unlimited)")
	httpMaxConnsPerIP := fs.Int("http-max-connections-per-ip", 0, "Max concurrent HTTP transport connections per remote IP (0 = unlimited)")
	httpRateLimitPerIP := fs.Int("http-rate-limit-per-ip", 0, "HTTP requests per second per remote IP (0 = unlimited)")
	httpRateLimitBurst := fs.Int("http-rate-limit-burst", 0, "HTTP per-IP rate-limit burst size (0 = same as rate)")
	httpCORSOrigins := fs.String("http-cors-allowed-origins", "", "Comma-separated allowed CORS origins for the HTTP API (empty = disabled)")
	debug := fs.Bool("debug", false, "Enable debug logging")
	version := fs.Bool("version", false, "Print version and exit")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	r := newResolver(fs)

	authTok, err := r.loadAuthToken(*authToken, *authTokenFile)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Host:                   r.str("host", "DFLOCKD_HOST", *host),
		TLSCert:                r.str("tls-cert", "DFLOCKD_TLS_CERT", *tlsCert),
		TLSKey:                 r.str("tls-key", "DFLOCKD_TLS_KEY", *tlsKey),
		HTTPHost:               r.str("http-host", "DFLOCKD_HTTP_HOST", *httpHost),
		AuthToken:              authTok,
		HTTPCORSAllowedOrigins: splitCSV(r.str("http-cors-allowed-origins", "DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS", *httpCORSOrigins)),
		Version:                *version,
	}
	for _, fn := range []func(*Config) error{
		func(c *Config) error {
			var err error
			c.Port, err = r.intVal("port", "DFLOCKD_PORT", *port)
			return err
		},
		func(c *Config) error {
			var err error
			c.MaxLocks, err = r.intVal("max-locks", "DFLOCKD_MAX_LOCKS", *maxLocks)
			return err
		},
		func(c *Config) error {
			var err error
			c.MaxConnections, err = r.intVal("max-connections", "DFLOCKD_MAX_CONNECTIONS", *maxConnections)
			return err
		},
		func(c *Config) error {
			var err error
			c.MaxConnectionsPerIP, err = r.intVal("max-connections-per-ip", "DFLOCKD_MAX_CONNECTIONS_PER_IP", *maxConnectionsPerIP)
			return err
		},
		func(c *Config) error {
			var err error
			c.MaxWaiters, err = r.intVal("max-waiters", "DFLOCKD_MAX_WAITERS", *maxWaiters)
			return err
		},
		func(c *Config) error {
			var err error
			c.AutoReleaseOnDisconnect, err = r.boolVal("auto-release-on-disconnect", "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", *autoRelease)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPPort, err = r.intVal("http-port", "DFLOCKD_HTTP_PORT", *httpPort)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPMaxSessions, err = r.intVal("http-max-sessions", "DFLOCKD_HTTP_MAX_SESSIONS", *httpMaxSessions)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPMaxSessionsPerIP, err = r.intVal("http-max-sessions-per-ip", "DFLOCKD_HTTP_MAX_SESSIONS_PER_IP", *httpMaxSessionsPerIP)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPMaxConnectionsPerIP, err = r.intVal("http-max-connections-per-ip", "DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP", *httpMaxConnsPerIP)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPRateLimitPerIP, err = r.intVal("http-rate-limit-per-ip", "DFLOCKD_HTTP_RATE_LIMIT_PER_IP", *httpRateLimitPerIP)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPRateLimitBurst, err = r.intVal("http-rate-limit-burst", "DFLOCKD_HTTP_RATE_LIMIT_BURST", *httpRateLimitBurst)
			return err
		},
		func(c *Config) error {
			var err error
			c.Debug, err = r.boolVal("debug", "DFLOCKD_DEBUG", *debug)
			return err
		},
		func(c *Config) error {
			var err error
			c.DefaultLeaseTTL, err = r.duration("default-lease-ttl", "DFLOCKD_DEFAULT_LEASE_TTL_S", *defaultLeaseTTL)
			return err
		},
		func(c *Config) error {
			var err error
			c.LeaseSweepInterval, err = r.duration("lease-sweep-interval", "DFLOCKD_LEASE_SWEEP_INTERVAL_S", *leaseSweepInterval)
			return err
		},
		func(c *Config) error {
			var err error
			c.GCInterval, err = r.duration("gc-interval", "DFLOCKD_GC_INTERVAL_S", *gcInterval)
			return err
		},
		func(c *Config) error {
			var err error
			c.GCMaxIdleTime, err = r.duration("gc-max-idle", "DFLOCKD_GC_MAX_IDLE_S", *gcMaxIdle)
			return err
		},
		func(c *Config) error {
			var err error
			c.ReadTimeout, err = r.duration("read-timeout", "DFLOCKD_READ_TIMEOUT_S", *readTimeout)
			return err
		},
		func(c *Config) error {
			var err error
			c.WriteTimeout, err = r.duration("write-timeout", "DFLOCKD_WRITE_TIMEOUT_S", *writeTimeout)
			return err
		},
		func(c *Config) error {
			var err error
			c.ShutdownTimeout, err = r.duration("shutdown-timeout", "DFLOCKD_SHUTDOWN_TIMEOUT_S", *shutdownTimeout)
			return err
		},
		func(c *Config) error {
			var err error
			c.HTTPSessionIdleTimeout, err = r.duration("http-session-idle-timeout", "DFLOCKD_HTTP_SESSION_IDLE_S", *httpIdle)
			return err
		},
	} {
		if err := fn(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.HTTPRateLimitPerIP > 0 && cfg.HTTPRateLimitBurst == 0 {
		cfg.HTTPRateLimitBurst = cfg.HTTPRateLimitPerIP
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate enforces invariants that aren't expressible as simple flag
// types. Returned errors are wrapped to identify the offending flag.
func (c *Config) Validate() error {
	if c.MaxLocks <= 0 {
		return fmt.Errorf("--max-locks must be > 0 (got %d)", c.MaxLocks)
	}
	if c.DefaultLeaseTTL <= 0 {
		return fmt.Errorf("--default-lease-ttl must be > 0")
	}
	if c.LeaseSweepInterval <= 0 {
		return fmt.Errorf("--lease-sweep-interval must be > 0")
	}
	if c.GCInterval <= 0 {
		return fmt.Errorf("--gc-interval must be > 0")
	}
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("--read-timeout must be > 0")
	}
	if c.WriteTimeout < 0 {
		return fmt.Errorf("--write-timeout must be >= 0")
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("--shutdown-timeout must be >= 0")
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("--port must be 0-65535 (got %d)", c.Port)
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("--max-connections must be >= 0")
	}
	if c.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("--max-connections-per-ip must be >= 0")
	}
	if c.MaxWaiters < 0 {
		return fmt.Errorf("--max-waiters must be >= 0")
	}
	if c.GCMaxIdleTime < 0 {
		return fmt.Errorf("--gc-max-idle must be >= 0")
	}
	if (c.TLSCert != "") != (c.TLSKey != "") {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	if strings.ContainsAny(c.AuthToken, "\n\r") {
		return fmt.Errorf("auth token must not contain newline characters")
	}
	if c.HTTPPort < 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("--http-port must be 0-65535 (got %d)", c.HTTPPort)
	}
	if c.HTTPPort != 0 && c.HTTPPort == c.Port {
		return fmt.Errorf("--http-port (%d) must differ from --port", c.HTTPPort)
	}
	if c.HTTPSessionIdleTimeout < 0 {
		return fmt.Errorf("--http-session-idle-timeout must be >= 0")
	}
	if c.HTTPMaxSessions < 0 {
		return fmt.Errorf("--http-max-sessions must be >= 0")
	}
	if c.HTTPMaxSessionsPerIP < 0 {
		return fmt.Errorf("--http-max-sessions-per-ip must be >= 0")
	}
	if c.HTTPMaxConnectionsPerIP < 0 {
		return fmt.Errorf("--http-max-connections-per-ip must be >= 0")
	}
	if c.HTTPRateLimitPerIP < 0 {
		return fmt.Errorf("--http-rate-limit-per-ip must be >= 0")
	}
	if c.HTTPRateLimitBurst < 0 {
		return fmt.Errorf("--http-rate-limit-burst must be >= 0")
	}
	if c.HTTPRateLimitPerIP > 0 && c.HTTPRateLimitBurst == 0 {
		return fmt.Errorf("--http-rate-limit-burst must be > 0 when --http-rate-limit-per-ip is set")
	}
	return nil
}

// resolver implements the precedence rules: explicit CLI flag wins over
// env, env wins over flag default. Track which flags the user actually
// set so unset flags don't shadow legitimate env values.
type resolver struct {
	setFlags map[string]bool
}

func newResolver(fs *flag.FlagSet) *resolver {
	r := &resolver{setFlags: make(map[string]bool)}
	fs.Visit(func(f *flag.Flag) { r.setFlags[f.Name] = true })
	return r
}

func (r *resolver) str(flag, env, def string) string {
	if r.setFlags[flag] {
		return def
	}
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

func (r *resolver) intVal(flag, env string, def int) (int, error) {
	if r.setFlags[flag] {
		return def, nil
	}
	if v := os.Getenv(env); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer (got %q)", env, v)
		}
		return n, nil
	}
	return def, nil
}

func (r *resolver) boolVal(flag, env string, def bool) (bool, error) {
	if r.setFlags[flag] {
		return def, nil
	}
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	switch strings.ToLower(v) {
	case "1", "yes", "true":
		return true, nil
	case "0", "no", "false":
		return false, nil
	}
	return false, fmt.Errorf("%s must be a boolean (got %q)", env, v)
}

func (r *resolver) duration(flag, env string, def int) (time.Duration, error) {
	n, err := r.intVal(flag, env, def)
	if err != nil {
		return 0, err
	}
	return secondsToDuration(env, n)
}

// loadAuthToken resolves the auth token following the documented
// precedence: --auth-token > --auth-token-file > DFLOCKD_AUTH_TOKEN
// > DFLOCKD_AUTH_TOKEN_FILE.
func (r *resolver) loadAuthToken(flagToken, flagTokenFile string) (string, error) {
	if r.setFlags["auth-token"] && flagToken != "" {
		return cleanAuthToken("--auth-token", flagToken)
	}
	if r.setFlags["auth-token-file"] && flagTokenFile != "" {
		return readAuthTokenFile(flagTokenFile)
	}
	if v := os.Getenv("DFLOCKD_AUTH_TOKEN"); v != "" {
		return cleanAuthToken("DFLOCKD_AUTH_TOKEN", v)
	}
	if v := os.Getenv("DFLOCKD_AUTH_TOKEN_FILE"); v != "" {
		return readAuthTokenFile(v)
	}
	return "", nil
}

func cleanAuthToken(source, token string) (string, error) {
	tok := strings.TrimSpace(token)
	if tok == "" {
		return "", fmt.Errorf("%s is empty", source)
	}
	if strings.ContainsAny(tok, "\n\r") {
		return "", fmt.Errorf("%s must not contain newline characters", source)
	}
	return tok, nil
}

func readAuthTokenFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading auth token file %q: %w", path, err)
	}
	return cleanAuthToken(fmt.Sprintf("auth token file %q", path), string(data))
}

func secondsToDuration(label string, seconds int) (time.Duration, error) {
	n := int64(seconds)
	if n > maxDurationSeconds || n < -maxDurationSeconds {
		return 0, fmt.Errorf("%s too large (max %d seconds)", label, maxDurationSeconds)
	}
	return time.Duration(n) * time.Second, nil
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
