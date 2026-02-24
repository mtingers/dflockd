package config

import (
	"flag"
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
// default if the env var is unset.
func envOrBool(envKey string, flagVal bool) bool {
	v := os.Getenv(envKey)
	if v == "" {
		return flagVal
	}
	low := strings.ToLower(v)
	return low == "1" || low == "yes" || low == "true"
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
func loadAuthToken(flagToken, flagTokenFile string) string {
	// Env var takes highest priority
	if v := os.Getenv("DFLOCKD_AUTH_TOKEN"); v != "" {
		return v
	}
	// Explicit flag value
	if flagToken != "" {
		return flagToken
	}
	// File-based token (flag)
	path := flagTokenFile
	if path == "" {
		path = os.Getenv("DFLOCKD_AUTH_TOKEN_FILE")
	}
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	return ""
}

func Load() *Config {
	// CLI flags
	host := flag.String("host", "127.0.0.1", "Bind address")
	port := flag.Int("port", 6388, "Bind port")
	defaultLeaseTTL := flag.Int("default-lease-ttl", 33, "Default lock lease duration (seconds)")
	leaseSweepInterval := flag.Int("lease-sweep-interval", 1, "Lease expiry check interval (seconds)")
	gcInterval := flag.Int("gc-interval", 5, "Lock state GC interval (seconds)")
	gcMaxIdle := flag.Int("gc-max-idle", 60, "Idle seconds before pruning lock state")
	maxLocks := flag.Int("max-locks", 1024, "Maximum number of unique lock keys")
	maxConnections := flag.Int("max-connections", 0, "Maximum concurrent connections (0 = unlimited)")
	maxWaiters := flag.Int("max-waiters", 0, "Maximum waiters per lock/semaphore key (0 = unlimited)")
	readTimeout := flag.Int("read-timeout", 23, "Client read timeout (seconds)")
	writeTimeout := flag.Int("write-timeout", 5, "Client write timeout (seconds)")
	shutdownTimeout := flag.Int("shutdown-timeout", 30, "Graceful shutdown drain timeout (seconds, 0 = wait forever)")
	autoRelease := flag.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	tlsCert := flag.String("tls-cert", "", "Path to TLS certificate PEM file")
	tlsKey := flag.String("tls-key", "", "Path to TLS private key PEM file")
	authToken := flag.String("auth-token", "", "Shared secret token for client authentication (visible in process list; prefer --auth-token-file)")
	authTokenFile := flag.String("auth-token-file", "", "Path to file containing the auth token (one line, trailing whitespace stripped)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	version := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	return &Config{
		Host:                    envOrString("DFLOCKD_HOST", *host),
		Port:                    envOrInt("DFLOCKD_PORT", *port),
		DefaultLeaseTTL:         envOrDuration("DFLOCKD_DEFAULT_LEASE_TTL_S", *defaultLeaseTTL),
		LeaseSweepInterval:      envOrDuration("DFLOCKD_LEASE_SWEEP_INTERVAL_S", *leaseSweepInterval),
		GCInterval:              envOrDuration("DFLOCKD_GC_LOOP_SLEEP", *gcInterval),
		GCMaxIdleTime:           envOrDuration("DFLOCKD_GC_MAX_UNUSED_TIME", *gcMaxIdle),
		MaxLocks:                envOrInt("DFLOCKD_MAX_LOCKS", *maxLocks),
		MaxConnections:          envOrInt("DFLOCKD_MAX_CONNECTIONS", *maxConnections),
		MaxWaiters:              envOrInt("DFLOCKD_MAX_WAITERS", *maxWaiters),
		ReadTimeout:             envOrDuration("DFLOCKD_READ_TIMEOUT_S", *readTimeout),
		WriteTimeout:            envOrDuration("DFLOCKD_WRITE_TIMEOUT_S", *writeTimeout),
		ShutdownTimeout:         envOrDuration("DFLOCKD_SHUTDOWN_TIMEOUT_S", *shutdownTimeout),
		AutoReleaseOnDisconnect: envOrBool("DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", *autoRelease),
		TLSCert:                 envOrString("DFLOCKD_TLS_CERT", *tlsCert),
		TLSKey:                  envOrString("DFLOCKD_TLS_KEY", *tlsKey),
		AuthToken:               loadAuthToken(*authToken, *authTokenFile),
		Debug:                   envOrBool("DFLOCKD_DEBUG", *debug),
		Version:                 *version,
	}
}
