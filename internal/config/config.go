package config

import (
	"flag"
	"fmt"
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
	MaxWaiters              int
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ShutdownTimeout         time.Duration
	AutoReleaseOnDisconnect bool
	Debug                   bool
	Version                 bool
	TLSCert                 string
	TLSKey                  string
	AuthToken               string
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

// envOrDuration returns a time.Duration in seconds from the environment
// variable, or converts the flag default (in seconds) if the env var is unset.
func envOrDuration(envKey string, flagVal int) time.Duration {
	return time.Duration(envOrInt(envKey, flagVal)) * time.Second
}

// loadAuthToken resolves the auth token from (in priority order):
//  1. DFLOCKD_AUTH_TOKEN env var
//  2. --auth-token flag
//  3. contents of --auth-token-file (trailing whitespace stripped)
//  4. contents of DFLOCKD_AUTH_TOKEN_FILE env var
func loadAuthToken(flagToken, flagTokenFile string) (string, error) {
	// Env var takes highest priority
	if v := os.Getenv("DFLOCKD_AUTH_TOKEN"); v != "" {
		return v, nil
	}
	// Explicit flag value
	if flagToken != "" {
		return flagToken, nil
	}
	// File-based token (flag or env)
	path := flagTokenFile
	if path == "" {
		path = os.Getenv("DFLOCKD_AUTH_TOKEN_FILE")
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("reading auth token file %q: %w", path, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return "", nil
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
	maxWaiters := fs.Int("max-waiters", 0, "Maximum waiters per lock/semaphore key (0 = unlimited)")
	readTimeout := fs.Int("read-timeout", 23, "Client read timeout (seconds)")
	writeTimeout := fs.Int("write-timeout", 5, "Client write timeout (seconds)")
	shutdownTimeout := fs.Int("shutdown-timeout", 30, "Graceful shutdown drain timeout (seconds, 0 = wait forever)")
	autoRelease := fs.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	tlsCert := fs.String("tls-cert", "", "Path to TLS certificate PEM file")
	tlsKey := fs.String("tls-key", "", "Path to TLS private key PEM file")
	authToken := fs.String("auth-token", "", "Shared secret token for client authentication (visible in process list; prefer --auth-token-file)")
	authTokenFile := fs.String("auth-token-file", "", "Path to file containing the auth token (one line, trailing whitespace stripped)")
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
	resolveDuration := func(flagName, envKey string, flagVal int) time.Duration {
		if setFlags[flagName] {
			return time.Duration(flagVal) * time.Second
		}
		return envOrDuration(envKey, flagVal)
	}

	authTok, err := loadAuthToken(*authToken, *authTokenFile)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Host:                    resolveString("host", "DFLOCKD_HOST", *host),
		Port:                    resolveInt("port", "DFLOCKD_PORT", *port),
		DefaultLeaseTTL:         resolveDuration("default-lease-ttl", "DFLOCKD_DEFAULT_LEASE_TTL_S", *defaultLeaseTTL),
		LeaseSweepInterval:      resolveDuration("lease-sweep-interval", "DFLOCKD_LEASE_SWEEP_INTERVAL_S", *leaseSweepInterval),
		GCInterval:              resolveDuration("gc-interval", "DFLOCKD_GC_LOOP_SLEEP", *gcInterval),
		GCMaxIdleTime:           resolveDuration("gc-max-idle", "DFLOCKD_GC_MAX_UNUSED_TIME", *gcMaxIdle),
		MaxLocks:                resolveInt("max-locks", "DFLOCKD_MAX_LOCKS", *maxLocks),
		MaxConnections:          resolveInt("max-connections", "DFLOCKD_MAX_CONNECTIONS", *maxConnections),
		MaxWaiters:              resolveInt("max-waiters", "DFLOCKD_MAX_WAITERS", *maxWaiters),
		ReadTimeout:             resolveDuration("read-timeout", "DFLOCKD_READ_TIMEOUT_S", *readTimeout),
		WriteTimeout:            resolveDuration("write-timeout", "DFLOCKD_WRITE_TIMEOUT_S", *writeTimeout),
		ShutdownTimeout:         resolveDuration("shutdown-timeout", "DFLOCKD_SHUTDOWN_TIMEOUT_S", *shutdownTimeout),
		AutoReleaseOnDisconnect: resolveBool("auto-release-on-disconnect", "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", *autoRelease),
		TLSCert:                 resolveString("tls-cert", "DFLOCKD_TLS_CERT", *tlsCert),
		TLSKey:                  resolveString("tls-key", "DFLOCKD_TLS_KEY", *tlsKey),
		AuthToken:               authTok,
		Debug:                   resolveBool("debug", "DFLOCKD_DEBUG", *debug),
		Version:                 *version,
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
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
	if c.MaxWaiters < 0 {
		return fmt.Errorf("--max-waiters must be >= 0 (got %d)", c.MaxWaiters)
	}
	if c.GCMaxIdleTime <= 0 {
		return fmt.Errorf("--gc-max-idle must be > 0 (got %s)", c.GCMaxIdleTime)
	}
	return nil
}
