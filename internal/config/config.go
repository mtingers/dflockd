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

type Config struct {
	Host                    string
	Port                    int
	DefaultLeaseTTL         time.Duration
	LeaseSweepInterval      time.Duration
	GCInterval              time.Duration
	GCMaxIdleTime           time.Duration
	MaxLocks                int
	MaxConnections          int
	MaxConnectionsPerIP     int
	MaxWaiters              int
	MaxSubscriptions        int
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ShutdownTimeout         time.Duration
	AutoReleaseOnDisconnect bool
	Debug                   bool
	Version                 bool
	TLSCert                 string
	TLSKey                  string
	AuthToken               string

	// HTTP API (opt-in; HTTPPort=0 disables).
	HTTPPort                int
	HTTPHost                string
	HTTPSessionIdleTimeout  time.Duration
	HTTPMaxSessions         int
	HTTPMaxSessionsPerIP    int
	HTTPMaxConnectionsPerIP int
	HTTPRateLimitPerIP      int
	HTTPRateLimitBurst      int
	HTTPSSEPingInterval     time.Duration
	HTTPCORSAllowedOrigins  []string

	// Replication (opt-in; ReplicationRole=="" disables).
	ReplicationRole       string        // "primary" | "secondary" | "" (standalone)
	ReplicationPeerAddr   string        // primary: where the secondary listens; secondary: ignored
	ReplicationListenAddr string        // secondary: where the primary connects; primary: ignored
	ReplicationMaxPause   time.Duration // 0 → 5s
	ReplicationNodeID     string        // optional; auto-derived from --host:--port if empty
}

// envLookup returns the first non-empty env value from the given keys,
// or "" if none are set. Used for canonical-plus-deprecated name lookup.
// First key should be the canonical (new) name; subsequent keys are
// deprecated aliases kept for backward compatibility.
func envLookup(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// envOrInt returns the environment variable value parsed as int, or the flag
// default if the env var is unset or unparseable.
func envOrInt(envKey string, flagVal int) int {
	v := os.Getenv(envKey)
	if v == "" {
		return flagVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return flagVal
	}
	return n
}

// envOrBool returns the environment variable value parsed as bool, or the flag
// default if the env var is unset. Recognizes 1/yes/true as true and
// 0/no/false as false; unrecognized values fall back to the flag default.
func envOrBool(envKey string, flagVal bool) bool {
	v := os.Getenv(envKey)
	if v == "" {
		return flagVal
	}
	switch strings.ToLower(v) {
	case "1", "yes", "true":
		return true
	case "0", "no", "false":
		return false
	default:
		return flagVal
	}
}

// envOrString returns the environment variable value, or the flag default if
// the env var is unset.
func envOrString(envKey string, flagVal string) string {
	v := os.Getenv(envKey)
	if v == "" {
		return flagVal
	}
	return v
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

const maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)

func durationFromSeconds(label string, seconds int) (time.Duration, error) {
	n := int64(seconds)
	if n > maxDurationSeconds || n < -maxDurationSeconds {
		return 0, fmt.Errorf("%s too large (max %d seconds)", label, maxDurationSeconds)
	}
	return time.Duration(n) * time.Second, nil
}

// envOrDuration returns a time.Duration in seconds from the environment
// variable, or converts the flag default (in seconds) if the env var is unset.
func envOrDuration(envKey string, flagVal int) (time.Duration, error) {
	return durationFromSeconds(envKey, envOrInt(envKey, flagVal))
}

// envOrDurationWithAliases is envOrDuration that also looks up deprecated
// env var names. Used to migrate from legacy names (DFLOCKD_GC_LOOP_SLEEP,
// DFLOCKD_GC_MAX_UNUSED_TIME) to the canonical-matching-flag names
// (DFLOCKD_GC_INTERVAL, DFLOCKD_GC_MAX_IDLE). Reports which name was used
// via the returned string so the caller can log a deprecation warning.
func envOrDurationWithAliases(flagVal int, keys ...string) (time.Duration, string, error) {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				continue
			}
			d, err := durationFromSeconds(k, n)
			return d, k, err
		}
	}
	d, err := durationFromSeconds(keys[0], flagVal)
	return d, "", err
}

// loadAuthToken resolves the auth token following the same precedence the
// rest of the config uses (README: "CLI flags take precedence over
// environment variables"):
//
//  1. --auth-token flag (if explicitly set)
//  2. --auth-token-file flag (if explicitly set)
//  3. DFLOCKD_AUTH_TOKEN env var
//  4. DFLOCKD_AUTH_TOKEN_FILE env var
//
// flagTokenSet and flagTokenFileSet tell us whether the caller actually
// passed the flag (vs receiving the empty default), so an empty flag
// value doesn't accidentally override a set env var.
func loadAuthToken(flagToken string, flagTokenSet bool, flagTokenFile string, flagTokenFileSet bool) (string, error) {
	if flagTokenSet && flagToken != "" {
		return cleanAuthToken("--auth-token", flagToken)
	}
	if flagTokenFileSet && flagTokenFile != "" {
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
	tok, err := cleanAuthToken(fmt.Sprintf("auth token file %q", path), string(data))
	if err != nil {
		return "", err
	}
	return tok, nil
}

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
	maxSubscriptions := fs.Int("max-subscriptions", 0, "Maximum watch+listen registrations per connection (0 = unlimited)")
	readTimeout := fs.Int("read-timeout", 23, "Client read timeout (seconds)")
	writeTimeout := fs.Int("write-timeout", 5, "Client write timeout (seconds)")
	shutdownTimeout := fs.Int("shutdown-timeout", 30, "Graceful shutdown drain timeout (seconds, 0 = wait forever)")
	autoRelease := fs.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	tlsCert := fs.String("tls-cert", "", "Path to TLS certificate PEM file")
	tlsKey := fs.String("tls-key", "", "Path to TLS private key PEM file")
	authToken := fs.String("auth-token", "", "Shared secret token for client authentication (visible in process list; prefer --auth-token-file)")
	authTokenFile := fs.String("auth-token-file", "", "Path to file containing the auth token (one line, trailing whitespace stripped)")
	httpPort := fs.Int("http-port", 0, "HTTP API listen port (0 = disabled)")
	httpHost := fs.String("http-host", "", "HTTP API bind address (defaults to --host)")
	httpIdle := fs.Int("http-session-idle-timeout", 20, "HTTP session idle timeout (seconds)")
	httpMaxSessions := fs.Int("http-max-sessions", 0, "Max concurrent HTTP sessions (0 = unlimited)")
	httpMaxSessionsPerIP := fs.Int("http-max-sessions-per-ip", 0, "Max concurrent HTTP sessions per remote IP (0 = unlimited)")
	httpMaxConnsPerIP := fs.Int("http-max-connections-per-ip", 0, "Max concurrent HTTP transport connections per remote IP (0 = unlimited)")
	httpRateLimitPerIP := fs.Int("http-rate-limit-per-ip", 0, "HTTP requests per second per remote IP (0 = unlimited)")
	httpRateLimitBurst := fs.Int("http-rate-limit-burst", 0, "HTTP per-IP rate-limit burst size (0 = same as rate)")
	httpSSEPing := fs.Int("http-sse-ping-interval", 15, "Internal ping interval for SSE streams (seconds)")
	httpCORSOrigins := fs.String("http-cors-allowed-origins", "", "Comma-separated allowed CORS origins for the HTTP API (empty = disabled)")
	replicationRole := fs.String("replication-role", "", "Replication role: 'primary', 'secondary', or '' (standalone)")
	replicationPeerAddr := fs.String("replication-peer", "", "Primary only: address (host:port) of the secondary's replication listener")
	replicationListenAddr := fs.String("replication-listen", "", "Secondary only: address (host:port) to listen on for the primary's replication connection")
	replicationMaxPauseMS := fs.Int("max-pause-ms", 5000, "Replication: ms with no peer contact before primary self-promotes to solo")
	replicationNodeID := fs.String("replication-node-id", "", "Replication: free-form node identifier (auto-derived if empty)")
	debug := fs.Bool("debug", false, "Enable debug logging")
	version := fs.Bool("version", false, "Print version and exit")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Track which flags were explicitly set on the command line so they
	// take precedence over environment variables.  Precedence order:
	//   CLI flag (explicit) > environment variable > flag default
	setFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = true
	})
	resolveInt := func(flagName, envKey string, flagVal int) int {
		if setFlags[flagName] {
			return flagVal
		}
		return envOrInt(envKey, flagVal)
	}
	resolveString := func(flagName, envKey string, flagVal string) string {
		if setFlags[flagName] {
			return flagVal
		}
		return envOrString(envKey, flagVal)
	}
	resolveBool := func(flagName, envKey string, flagVal bool) bool {
		if setFlags[flagName] {
			return flagVal
		}
		return envOrBool(envKey, flagVal)
	}
	resolveDuration := func(flagName, envKey string, flagVal int) (time.Duration, error) {
		if setFlags[flagName] {
			return durationFromSeconds("--"+flagName, flagVal)
		}
		return envOrDuration(envKey, flagVal)
	}
	// resolveDurationWithAliases prefers the canonical env var name, falls
	// back to one or more deprecated aliases, and prints a one-shot
	// deprecation warning to stderr when an alias matches. CLI flag still
	// wins when explicitly set.
	resolveDurationWithAliases := func(flagName string, flagVal int, keys ...string) (time.Duration, error) {
		if setFlags[flagName] {
			return durationFromSeconds("--"+flagName, flagVal)
		}
		d, matched, err := envOrDurationWithAliases(flagVal, keys...)
		if err != nil {
			return 0, err
		}
		if matched != "" && matched != keys[0] {
			fmt.Fprintf(os.Stderr,
				"dflockd: env var %s is deprecated; please use %s instead\n",
				matched, keys[0])
		}
		return d, nil
	}

	authTok, err := loadAuthToken(*authToken, setFlags["auth-token"], *authTokenFile, setFlags["auth-token-file"])
	if err != nil {
		return nil, err
	}

	defaultLeaseTTLDuration, err := resolveDuration("default-lease-ttl", "DFLOCKD_DEFAULT_LEASE_TTL_S", *defaultLeaseTTL)
	if err != nil {
		return nil, err
	}
	leaseSweepIntervalDuration, err := resolveDuration("lease-sweep-interval", "DFLOCKD_LEASE_SWEEP_INTERVAL_S", *leaseSweepInterval)
	if err != nil {
		return nil, err
	}
	gcIntervalDuration, err := resolveDurationWithAliases("gc-interval", *gcInterval,
		"DFLOCKD_GC_INTERVAL_S", "DFLOCKD_GC_LOOP_SLEEP")
	if err != nil {
		return nil, err
	}
	gcMaxIdleDuration, err := resolveDurationWithAliases("gc-max-idle", *gcMaxIdle,
		"DFLOCKD_GC_MAX_IDLE_S", "DFLOCKD_GC_MAX_UNUSED_TIME")
	if err != nil {
		return nil, err
	}
	readTimeoutDuration, err := resolveDuration("read-timeout", "DFLOCKD_READ_TIMEOUT_S", *readTimeout)
	if err != nil {
		return nil, err
	}
	writeTimeoutDuration, err := resolveDuration("write-timeout", "DFLOCKD_WRITE_TIMEOUT_S", *writeTimeout)
	if err != nil {
		return nil, err
	}
	shutdownTimeoutDuration, err := resolveDuration("shutdown-timeout", "DFLOCKD_SHUTDOWN_TIMEOUT_S", *shutdownTimeout)
	if err != nil {
		return nil, err
	}
	httpIdleDuration, err := resolveDuration("http-session-idle-timeout", "DFLOCKD_HTTP_SESSION_IDLE_S", *httpIdle)
	if err != nil {
		return nil, err
	}
	httpSSEPingDuration, err := resolveDuration("http-sse-ping-interval", "DFLOCKD_HTTP_SSE_PING_S", *httpSSEPing)
	if err != nil {
		return nil, err
	}

	resolvedHTTPRate := resolveInt("http-rate-limit-per-ip", "DFLOCKD_HTTP_RATE_LIMIT_PER_IP", *httpRateLimitPerIP)
	resolvedHTTPBurst := resolveInt("http-rate-limit-burst", "DFLOCKD_HTTP_RATE_LIMIT_BURST", *httpRateLimitBurst)
	if resolvedHTTPRate > 0 && resolvedHTTPBurst == 0 {
		resolvedHTTPBurst = resolvedHTTPRate
	}

	cfg := &Config{
		Host:                    resolveString("host", "DFLOCKD_HOST", *host),
		Port:                    resolveInt("port", "DFLOCKD_PORT", *port),
		DefaultLeaseTTL:         defaultLeaseTTLDuration,
		LeaseSweepInterval:      leaseSweepIntervalDuration,
		GCInterval:              gcIntervalDuration,
		GCMaxIdleTime:           gcMaxIdleDuration,
		MaxLocks:                resolveInt("max-locks", "DFLOCKD_MAX_LOCKS", *maxLocks),
		MaxConnections:          resolveInt("max-connections", "DFLOCKD_MAX_CONNECTIONS", *maxConnections),
		MaxConnectionsPerIP:     resolveInt("max-connections-per-ip", "DFLOCKD_MAX_CONNECTIONS_PER_IP", *maxConnectionsPerIP),
		MaxWaiters:              resolveInt("max-waiters", "DFLOCKD_MAX_WAITERS", *maxWaiters),
		MaxSubscriptions:        resolveInt("max-subscriptions", "DFLOCKD_MAX_SUBSCRIPTIONS", *maxSubscriptions),
		ReadTimeout:             readTimeoutDuration,
		WriteTimeout:            writeTimeoutDuration,
		ShutdownTimeout:         shutdownTimeoutDuration,
		AutoReleaseOnDisconnect: resolveBool("auto-release-on-disconnect", "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", *autoRelease),
		TLSCert:                 resolveString("tls-cert", "DFLOCKD_TLS_CERT", *tlsCert),
		TLSKey:                  resolveString("tls-key", "DFLOCKD_TLS_KEY", *tlsKey),
		AuthToken:               authTok,
		HTTPPort:                resolveInt("http-port", "DFLOCKD_HTTP_PORT", *httpPort),
		HTTPHost:                resolveString("http-host", "DFLOCKD_HTTP_HOST", *httpHost),
		HTTPSessionIdleTimeout:  httpIdleDuration,
		HTTPMaxSessions:         resolveInt("http-max-sessions", "DFLOCKD_HTTP_MAX_SESSIONS", *httpMaxSessions),
		HTTPMaxSessionsPerIP:    resolveInt("http-max-sessions-per-ip", "DFLOCKD_HTTP_MAX_SESSIONS_PER_IP", *httpMaxSessionsPerIP),
		HTTPMaxConnectionsPerIP: resolveInt("http-max-connections-per-ip", "DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP", *httpMaxConnsPerIP),
		HTTPRateLimitPerIP:      resolvedHTTPRate,
		HTTPRateLimitBurst:      resolvedHTTPBurst,
		HTTPSSEPingInterval:     httpSSEPingDuration,
		HTTPCORSAllowedOrigins:  splitCSV(resolveString("http-cors-allowed-origins", "DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS", *httpCORSOrigins)),
		ReplicationRole:         resolveString("replication-role", "DFLOCKD_REPLICATION_ROLE", *replicationRole),
		ReplicationPeerAddr:     resolveString("replication-peer", "DFLOCKD_REPLICATION_PEER", *replicationPeerAddr),
		ReplicationListenAddr:   resolveString("replication-listen", "DFLOCKD_REPLICATION_LISTEN", *replicationListenAddr),
		ReplicationMaxPause:     time.Duration(resolveInt("max-pause-ms", "DFLOCKD_MAX_PAUSE_MS", *replicationMaxPauseMS)) * time.Millisecond,
		ReplicationNodeID:       resolveString("replication-node-id", "DFLOCKD_REPLICATION_NODE_ID", *replicationNodeID),
		Debug:                   resolveBool("debug", "DFLOCKD_DEBUG", *debug),
		Version:                 *version,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

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
		return fmt.Errorf("--write-timeout must be >= 0 (got %s)", c.WriteTimeout)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("--shutdown-timeout must be >= 0 (got %s)", c.ShutdownTimeout)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("--port must be 0-65535 (got %d)", c.Port)
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("--max-connections must be >= 0 (got %d)", c.MaxConnections)
	}
	if c.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("--max-connections-per-ip must be >= 0 (got %d)", c.MaxConnectionsPerIP)
	}
	if c.MaxWaiters < 0 {
		return fmt.Errorf("--max-waiters must be >= 0 (got %d)", c.MaxWaiters)
	}
	if c.MaxSubscriptions < 0 {
		return fmt.Errorf("--max-subscriptions must be >= 0 (got %d)", c.MaxSubscriptions)
	}
	if c.GCMaxIdleTime < 0 {
		return fmt.Errorf("--gc-max-idle must be >= 0 (got %s)", c.GCMaxIdleTime)
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
	if c.HTTPSSEPingInterval < 0 {
		return fmt.Errorf("--http-sse-ping-interval must be >= 0")
	}
	// The SSE handler relies on its internal pinger to refresh the
	// session's lastSeen before the bridge sweeper reaps it. The sweeper
	// cutoff is 2 * HTTPSessionIdleTimeout, so a ping firing later than
	// that races the sweeper. Require pingInterval < idleTimeout to keep
	// 2x margin.
	if c.HTTPSSEPingInterval > 0 && c.HTTPSessionIdleTimeout > 0 &&
		c.HTTPSSEPingInterval >= c.HTTPSessionIdleTimeout {
		return fmt.Errorf("--http-sse-ping-interval (%s) must be less than --http-session-idle-timeout (%s)",
			c.HTTPSSEPingInterval, c.HTTPSessionIdleTimeout)
	}
	switch c.ReplicationRole {
	case "", "primary", "secondary":
	default:
		return fmt.Errorf("--replication-role must be one of: primary, secondary, '' (got %q)", c.ReplicationRole)
	}
	if c.ReplicationRole == "primary" && c.ReplicationPeerAddr == "" {
		return fmt.Errorf("--replication-role=primary requires --replication-peer")
	}
	if c.ReplicationRole == "secondary" && c.ReplicationListenAddr == "" {
		return fmt.Errorf("--replication-role=secondary requires --replication-listen")
	}
	if c.ReplicationMaxPause < 0 {
		return fmt.Errorf("--max-pause-ms must be >= 0")
	}
	return nil
}
