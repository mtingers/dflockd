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
	ReadTimeout             time.Duration
	AutoReleaseOnDisconnect bool
	Debug                   bool
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	low := strings.ToLower(v)
	return low == "1" || low == "yes" || low == "true"
}

func envString(key string, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func Load() *Config {
	// CLI flags (defaults match Python)
	host := flag.String("host", "0.0.0.0", "Bind address")
	port := flag.Int("port", 6388, "Bind port")
	defaultLeaseTTL := flag.Int("default-lease-ttl", 33, "Default lock lease duration (seconds)")
	leaseSweepInterval := flag.Int("lease-sweep-interval", 1, "Lease expiry check interval (seconds)")
	gcInterval := flag.Int("gc-interval", 5, "Lock state GC interval (seconds)")
	gcMaxIdle := flag.Int("gc-max-idle", 60, "Idle seconds before pruning lock state")
	maxLocks := flag.Int("max-locks", 1024, "Maximum number of unique lock keys")
	readTimeout := flag.Int("read-timeout", 23, "Client read timeout (seconds)")
	autoRelease := flag.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Env vars take precedence over CLI flags
	cfg := &Config{}

	if os.Getenv("DFLOCKD_HOST") != "" {
		cfg.Host = envString("DFLOCKD_HOST", *host)
	} else {
		cfg.Host = *host
	}
	if os.Getenv("DFLOCKD_PORT") != "" {
		cfg.Port = envInt("DFLOCKD_PORT", *port)
	} else {
		cfg.Port = *port
	}
	if os.Getenv("DFLOCKD_DEFAULT_LEASE_TTL_S") != "" {
		cfg.DefaultLeaseTTL = time.Duration(envInt("DFLOCKD_DEFAULT_LEASE_TTL_S", *defaultLeaseTTL)) * time.Second
	} else {
		cfg.DefaultLeaseTTL = time.Duration(*defaultLeaseTTL) * time.Second
	}
	if os.Getenv("DFLOCKD_LEASE_SWEEP_INTERVAL_S") != "" {
		cfg.LeaseSweepInterval = time.Duration(envInt("DFLOCKD_LEASE_SWEEP_INTERVAL_S", *leaseSweepInterval)) * time.Second
	} else {
		cfg.LeaseSweepInterval = time.Duration(*leaseSweepInterval) * time.Second
	}
	if os.Getenv("DFLOCKD_GC_LOOP_SLEEP") != "" {
		cfg.GCInterval = time.Duration(envInt("DFLOCKD_GC_LOOP_SLEEP", *gcInterval)) * time.Second
	} else {
		cfg.GCInterval = time.Duration(*gcInterval) * time.Second
	}
	if os.Getenv("DFLOCKD_GC_MAX_UNUSED_TIME") != "" {
		cfg.GCMaxIdleTime = time.Duration(envInt("DFLOCKD_GC_MAX_UNUSED_TIME", *gcMaxIdle)) * time.Second
	} else {
		cfg.GCMaxIdleTime = time.Duration(*gcMaxIdle) * time.Second
	}
	if os.Getenv("DFLOCKD_MAX_LOCKS") != "" {
		cfg.MaxLocks = envInt("DFLOCKD_MAX_LOCKS", *maxLocks)
	} else {
		cfg.MaxLocks = *maxLocks
	}
	if os.Getenv("DFLOCKD_READ_TIMEOUT_S") != "" {
		cfg.ReadTimeout = time.Duration(envInt("DFLOCKD_READ_TIMEOUT_S", *readTimeout)) * time.Second
	} else {
		cfg.ReadTimeout = time.Duration(*readTimeout) * time.Second
	}
	if os.Getenv("DFLOCKD_AUTO_RELEASE_ON_DISCONNECT") != "" {
		cfg.AutoReleaseOnDisconnect = envBool("DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", *autoRelease)
	} else {
		cfg.AutoReleaseOnDisconnect = *autoRelease
	}
	if os.Getenv("DFLOCKD_DEBUG") != "" {
		cfg.Debug = envBool("DFLOCKD_DEBUG", *debug)
	} else {
		cfg.Debug = *debug
	}

	return cfg
}
