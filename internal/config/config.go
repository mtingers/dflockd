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

// Load parses args and returns the resolved Config.
func Load(args []string) (*Config, error) {
	fs, f := newFlagSet()
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return buildConfig(fs, f)
}

// buildConfig resolves flags+env into a Config and validates it.
func buildConfig(fs *flag.FlagSet, f *flagPtrs) (*Config, error) {
	r := newResolver(fs)
	cfg, err := resolveAll(r, f)
	if err != nil {
		return nil, err
	}
	applyDerivedDefaults(cfg)
	return cfg, cfg.Validate()
}

// resolveAll runs every resolver group and returns the populated Config.
func resolveAll(r *resolver, f *flagPtrs) (*Config, error) {
	cfg := &Config{Version: *f.version}
	for _, fn := range []func(*resolver, *flagPtrs, *Config) error{
		resolveStrings, resolveInts, resolveBools, resolveDurations, resolveAuth,
	} {
		if err := fn(r, f, cfg); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// applyDerivedDefaults fills in fields whose default depends on other
// already-resolved fields. Currently only the rate-limit burst.
func applyDerivedDefaults(c *Config) {
	if c.HTTPRateLimitPerIP > 0 && c.HTTPRateLimitBurst == 0 {
		c.HTTPRateLimitBurst = c.HTTPRateLimitPerIP
	}
}

// ---------------------------------------------------------------------------
// Flag definition
// ---------------------------------------------------------------------------

// flagPtrs holds every *T returned by flag.Set.* — Load fills it once,
// then resolvers read through the pointers as their flag default.
type flagPtrs struct {
	host, tlsCert, tlsKey                                 *string
	authToken, authTokenFile                              *string
	httpHost, httpCORSOrigins                             *string
	port, maxLocks                                        *int
	maxConnections, maxConnectionsPerIP, maxWaiters       *int
	defaultLeaseTTL, leaseSweepInterval                   *int
	gcInterval, gcMaxIdle                                 *int
	readTimeout, writeTimeout, shutdownTimeout            *int
	httpPort, httpIdle                                    *int
	httpMaxSessions, httpMaxSessionsPerIP                 *int
	httpMaxConnsPerIP                                     *int
	httpRateLimitPerIP, httpRateLimitBurst                *int
	autoRelease, debug, version                           *bool
}

// newFlagSet defines every flag and returns the FlagSet plus the
// pointer bundle. Long because of the flag enumeration; complexity 1.
func newFlagSet() (*flag.FlagSet, *flagPtrs) {
	fs := flag.NewFlagSet("dflockd", flag.ContinueOnError)
	f := &flagPtrs{}
	defineStringFlags(fs, f)
	defineIntFlags(fs, f)
	defineBoolFlags(fs, f)
	return fs, f
}

func defineStringFlags(fs *flag.FlagSet, f *flagPtrs) {
	f.host = fs.String("host", "127.0.0.1", "Bind address")
	f.tlsCert = fs.String("tls-cert", "", "Path to TLS certificate PEM file")
	f.tlsKey = fs.String("tls-key", "", "Path to TLS private key PEM file")
	f.authToken = fs.String("auth-token", "", "Shared secret token (visible in process list; prefer --auth-token-file)")
	f.authTokenFile = fs.String("auth-token-file", "", "Path to file containing the auth token (one line)")
	f.httpHost = fs.String("http-host", "", "HTTP API bind address (defaults to --host)")
	f.httpCORSOrigins = fs.String("http-cors-allowed-origins", "", "Comma-separated allowed CORS origins for the HTTP API (empty = disabled)")
}

func defineIntFlags(fs *flag.FlagSet, f *flagPtrs) {
	f.port = fs.Int("port", 6388, "Bind port")
	f.defaultLeaseTTL = fs.Int("default-lease-ttl", 33, "Default lock lease duration (seconds)")
	f.leaseSweepInterval = fs.Int("lease-sweep-interval", 1, "Lease expiry check interval (seconds)")
	f.gcInterval = fs.Int("gc-interval", 5, "Lock state GC interval (seconds)")
	f.gcMaxIdle = fs.Int("gc-max-idle", 60, "Idle seconds before pruning lock state")
	f.maxLocks = fs.Int("max-locks", 1024, "Maximum number of unique lock keys")
	f.maxConnections = fs.Int("max-connections", 0, "Maximum concurrent connections (0 = unlimited)")
	f.maxConnectionsPerIP = fs.Int("max-connections-per-ip", 0, "Maximum concurrent TCP connections per remote IP (0 = unlimited)")
	f.maxWaiters = fs.Int("max-waiters", 0, "Maximum waiters per lock/semaphore key (0 = unlimited)")
	f.readTimeout = fs.Int("read-timeout", 23, "Client read timeout (seconds)")
	f.writeTimeout = fs.Int("write-timeout", 5, "Client write timeout (seconds)")
	f.shutdownTimeout = fs.Int("shutdown-timeout", 30, "Graceful shutdown drain timeout (seconds, 0 = wait forever)")
	f.httpPort = fs.Int("http-port", 0, "HTTP API listen port (0 = disabled)")
	f.httpIdle = fs.Int("http-session-idle-timeout", 20, "HTTP session idle timeout (seconds)")
	f.httpMaxSessions = fs.Int("http-max-sessions", 0, "Max concurrent HTTP sessions (0 = unlimited)")
	f.httpMaxSessionsPerIP = fs.Int("http-max-sessions-per-ip", 0, "Max concurrent HTTP sessions per remote IP (0 = unlimited)")
	f.httpMaxConnsPerIP = fs.Int("http-max-connections-per-ip", 0, "Max concurrent HTTP transport connections per remote IP (0 = unlimited)")
	f.httpRateLimitPerIP = fs.Int("http-rate-limit-per-ip", 0, "HTTP requests per second per remote IP (0 = unlimited)")
	f.httpRateLimitBurst = fs.Int("http-rate-limit-burst", 0, "HTTP per-IP rate-limit burst size (0 = same as rate)")
}

func defineBoolFlags(fs *flag.FlagSet, f *flagPtrs) {
	f.autoRelease = fs.Bool("auto-release-on-disconnect", true, "Release locks when a client disconnects")
	f.debug = fs.Bool("debug", false, "Enable debug logging")
	f.version = fs.Bool("version", false, "Print version and exit")
}

// ---------------------------------------------------------------------------
// Resolvers — one function per primitive type, each driven by a table.
// ---------------------------------------------------------------------------

// stringResolver wires up one string-shaped resolution: the flag name,
// the env var, the default pointer, and the setter on Config.
type stringResolver struct {
	flag, env string
	def       *string
	set       func(*Config, string)
}

func resolveStrings(r *resolver, f *flagPtrs, c *Config) error {
	for _, x := range stringResolvers(f) {
		x.set(c, r.str(x.flag, x.env, *x.def))
	}
	c.HTTPCORSAllowedOrigins = splitCSV(r.str(
		"http-cors-allowed-origins", "DFLOCKD_HTTP_CORS_ALLOWED_ORIGINS", *f.httpCORSOrigins))
	return nil
}

func stringResolvers(f *flagPtrs) []stringResolver {
	return []stringResolver{
		{"host", "DFLOCKD_HOST", f.host, func(c *Config, v string) { c.Host = v }},
		{"tls-cert", "DFLOCKD_TLS_CERT", f.tlsCert, func(c *Config, v string) { c.TLSCert = v }},
		{"tls-key", "DFLOCKD_TLS_KEY", f.tlsKey, func(c *Config, v string) { c.TLSKey = v }},
		{"http-host", "DFLOCKD_HTTP_HOST", f.httpHost, func(c *Config, v string) { c.HTTPHost = v }},
	}
}

// intResolver wires up one int-shaped resolution. set may also be used
// for derived-bool / derived-duration callers via the helpers below.
type intResolver struct {
	flag, env string
	def       *int
	set       func(*Config, int)
}

func resolveInts(r *resolver, f *flagPtrs, c *Config) error {
	for _, x := range intResolvers(f) {
		v, err := r.intVal(x.flag, x.env, *x.def)
		if err != nil {
			return err
		}
		x.set(c, v)
	}
	return nil
}

func intResolvers(f *flagPtrs) []intResolver {
	return []intResolver{
		{"port", "DFLOCKD_PORT", f.port, func(c *Config, v int) { c.Port = v }},
		{"max-locks", "DFLOCKD_MAX_LOCKS", f.maxLocks, func(c *Config, v int) { c.MaxLocks = v }},
		{"max-connections", "DFLOCKD_MAX_CONNECTIONS", f.maxConnections, func(c *Config, v int) { c.MaxConnections = v }},
		{"max-connections-per-ip", "DFLOCKD_MAX_CONNECTIONS_PER_IP", f.maxConnectionsPerIP, func(c *Config, v int) { c.MaxConnectionsPerIP = v }},
		{"max-waiters", "DFLOCKD_MAX_WAITERS", f.maxWaiters, func(c *Config, v int) { c.MaxWaiters = v }},
		{"http-port", "DFLOCKD_HTTP_PORT", f.httpPort, func(c *Config, v int) { c.HTTPPort = v }},
		{"http-max-sessions", "DFLOCKD_HTTP_MAX_SESSIONS", f.httpMaxSessions, func(c *Config, v int) { c.HTTPMaxSessions = v }},
		{"http-max-sessions-per-ip", "DFLOCKD_HTTP_MAX_SESSIONS_PER_IP", f.httpMaxSessionsPerIP, func(c *Config, v int) { c.HTTPMaxSessionsPerIP = v }},
		{"http-max-connections-per-ip", "DFLOCKD_HTTP_MAX_CONNECTIONS_PER_IP", f.httpMaxConnsPerIP, func(c *Config, v int) { c.HTTPMaxConnectionsPerIP = v }},
		{"http-rate-limit-per-ip", "DFLOCKD_HTTP_RATE_LIMIT_PER_IP", f.httpRateLimitPerIP, func(c *Config, v int) { c.HTTPRateLimitPerIP = v }},
		{"http-rate-limit-burst", "DFLOCKD_HTTP_RATE_LIMIT_BURST", f.httpRateLimitBurst, func(c *Config, v int) { c.HTTPRateLimitBurst = v }},
	}
}

type boolResolver struct {
	flag, env string
	def       *bool
	set       func(*Config, bool)
}

func resolveBools(r *resolver, f *flagPtrs, c *Config) error {
	for _, x := range boolResolvers(f) {
		v, err := r.boolVal(x.flag, x.env, *x.def)
		if err != nil {
			return err
		}
		x.set(c, v)
	}
	return nil
}

func boolResolvers(f *flagPtrs) []boolResolver {
	return []boolResolver{
		{"auto-release-on-disconnect", "DFLOCKD_AUTO_RELEASE_ON_DISCONNECT", f.autoRelease, func(c *Config, v bool) { c.AutoReleaseOnDisconnect = v }},
		{"debug", "DFLOCKD_DEBUG", f.debug, func(c *Config, v bool) { c.Debug = v }},
	}
}

type durationResolver struct {
	flag, env string
	def       *int
	set       func(*Config, time.Duration)
}

func resolveDurations(r *resolver, f *flagPtrs, c *Config) error {
	for _, x := range durationResolvers(f) {
		v, err := r.duration(x.flag, x.env, *x.def)
		if err != nil {
			return err
		}
		x.set(c, v)
	}
	return nil
}

func durationResolvers(f *flagPtrs) []durationResolver {
	return []durationResolver{
		{"default-lease-ttl", "DFLOCKD_DEFAULT_LEASE_TTL_S", f.defaultLeaseTTL, func(c *Config, v time.Duration) { c.DefaultLeaseTTL = v }},
		{"lease-sweep-interval", "DFLOCKD_LEASE_SWEEP_INTERVAL_S", f.leaseSweepInterval, func(c *Config, v time.Duration) { c.LeaseSweepInterval = v }},
		{"gc-interval", "DFLOCKD_GC_INTERVAL_S", f.gcInterval, func(c *Config, v time.Duration) { c.GCInterval = v }},
		{"gc-max-idle", "DFLOCKD_GC_MAX_IDLE_S", f.gcMaxIdle, func(c *Config, v time.Duration) { c.GCMaxIdleTime = v }},
		{"read-timeout", "DFLOCKD_READ_TIMEOUT_S", f.readTimeout, func(c *Config, v time.Duration) { c.ReadTimeout = v }},
		{"write-timeout", "DFLOCKD_WRITE_TIMEOUT_S", f.writeTimeout, func(c *Config, v time.Duration) { c.WriteTimeout = v }},
		{"shutdown-timeout", "DFLOCKD_SHUTDOWN_TIMEOUT_S", f.shutdownTimeout, func(c *Config, v time.Duration) { c.ShutdownTimeout = v }},
		{"http-session-idle-timeout", "DFLOCKD_HTTP_SESSION_IDLE_S", f.httpIdle, func(c *Config, v time.Duration) { c.HTTPSessionIdleTimeout = v }},
	}
}

// resolveAuth resolves the auth token through its dedicated precedence
// chain (set inside the same Pass so AuthToken is populated for Validate).
func resolveAuth(r *resolver, f *flagPtrs, c *Config) error {
	tok, err := r.loadAuthToken(*f.authToken, *f.authTokenFile)
	if err != nil {
		return err
	}
	c.AuthToken = tok
	return nil
}

// ---------------------------------------------------------------------------
// Validate — list of named validators each returning one error or nil.
// ---------------------------------------------------------------------------

// Validate enforces invariants that aren't expressible as flag types.
func (c *Config) Validate() error {
	for _, v := range validators {
		if err := v(c); err != nil {
			return err
		}
	}
	return nil
}

// validators is the canonical list of single-purpose checks. Order is
// not load-bearing; tests may call any one in isolation.
var validators = []func(*Config) error{
	validateMaxLocks,
	validateDefaultLeaseTTL,
	validateLeaseSweepInterval,
	validateGCInterval,
	validateReadTimeout,
	validateWriteTimeout,
	validateShutdownTimeout,
	validatePort,
	validateMaxConnections,
	validateMaxConnectionsPerIP,
	validateMaxWaiters,
	validateGCMaxIdleTime,
	validateTLSPaired,
	validateAuthTokenChars,
	validateHTTPPortRange,
	validateHTTPPortDistinct,
	validateHTTPSessionIdle,
	validateHTTPMaxSessions,
	validateHTTPMaxSessionsPerIP,
	validateHTTPMaxConnsPerIP,
	validateHTTPRateLimitPerIP,
	validateHTTPRateLimitBurst,
	validateHTTPRateBurstWhenRate,
}

func validateMaxLocks(c *Config) error {
	if c.MaxLocks <= 0 {
		return fmt.Errorf("--max-locks must be > 0 (got %d)", c.MaxLocks)
	}
	return nil
}

func validateDefaultLeaseTTL(c *Config) error {
	if c.DefaultLeaseTTL <= 0 {
		return fmt.Errorf("--default-lease-ttl must be > 0")
	}
	return nil
}

func validateLeaseSweepInterval(c *Config) error {
	if c.LeaseSweepInterval <= 0 {
		return fmt.Errorf("--lease-sweep-interval must be > 0")
	}
	return nil
}

func validateGCInterval(c *Config) error {
	if c.GCInterval <= 0 {
		return fmt.Errorf("--gc-interval must be > 0")
	}
	return nil
}

func validateReadTimeout(c *Config) error {
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("--read-timeout must be > 0")
	}
	return nil
}

func validateWriteTimeout(c *Config) error {
	if c.WriteTimeout < 0 {
		return fmt.Errorf("--write-timeout must be >= 0")
	}
	return nil
}

func validateShutdownTimeout(c *Config) error {
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("--shutdown-timeout must be >= 0")
	}
	return nil
}

func validatePort(c *Config) error {
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("--port must be 0-65535 (got %d)", c.Port)
	}
	return nil
}

func validateMaxConnections(c *Config) error {
	if c.MaxConnections < 0 {
		return fmt.Errorf("--max-connections must be >= 0")
	}
	return nil
}

func validateMaxConnectionsPerIP(c *Config) error {
	if c.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("--max-connections-per-ip must be >= 0")
	}
	return nil
}

func validateMaxWaiters(c *Config) error {
	if c.MaxWaiters < 0 {
		return fmt.Errorf("--max-waiters must be >= 0")
	}
	return nil
}

func validateGCMaxIdleTime(c *Config) error {
	if c.GCMaxIdleTime < 0 {
		return fmt.Errorf("--gc-max-idle must be >= 0")
	}
	return nil
}

func validateTLSPaired(c *Config) error {
	if (c.TLSCert != "") != (c.TLSKey != "") {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	return nil
}

func validateAuthTokenChars(c *Config) error {
	if strings.ContainsAny(c.AuthToken, "\n\r") {
		return fmt.Errorf("auth token must not contain newline characters")
	}
	return nil
}

func validateHTTPPortRange(c *Config) error {
	if c.HTTPPort < 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("--http-port must be 0-65535 (got %d)", c.HTTPPort)
	}
	return nil
}

func validateHTTPPortDistinct(c *Config) error {
	if c.HTTPPort != 0 && c.HTTPPort == c.Port {
		return fmt.Errorf("--http-port (%d) must differ from --port", c.HTTPPort)
	}
	return nil
}

func validateHTTPSessionIdle(c *Config) error {
	if c.HTTPSessionIdleTimeout < 0 {
		return fmt.Errorf("--http-session-idle-timeout must be >= 0")
	}
	return nil
}

func validateHTTPMaxSessions(c *Config) error {
	if c.HTTPMaxSessions < 0 {
		return fmt.Errorf("--http-max-sessions must be >= 0")
	}
	return nil
}

func validateHTTPMaxSessionsPerIP(c *Config) error {
	if c.HTTPMaxSessionsPerIP < 0 {
		return fmt.Errorf("--http-max-sessions-per-ip must be >= 0")
	}
	return nil
}

func validateHTTPMaxConnsPerIP(c *Config) error {
	if c.HTTPMaxConnectionsPerIP < 0 {
		return fmt.Errorf("--http-max-connections-per-ip must be >= 0")
	}
	return nil
}

func validateHTTPRateLimitPerIP(c *Config) error {
	if c.HTTPRateLimitPerIP < 0 {
		return fmt.Errorf("--http-rate-limit-per-ip must be >= 0")
	}
	return nil
}

func validateHTTPRateLimitBurst(c *Config) error {
	if c.HTTPRateLimitBurst < 0 {
		return fmt.Errorf("--http-rate-limit-burst must be >= 0")
	}
	return nil
}

func validateHTTPRateBurstWhenRate(c *Config) error {
	if c.HTTPRateLimitPerIP > 0 && c.HTTPRateLimitBurst == 0 {
		return fmt.Errorf("--http-rate-limit-burst must be > 0 when --http-rate-limit-per-ip is set")
	}
	return nil
}

// ---------------------------------------------------------------------------
// resolver — looks up flag-vs-env precedence
// ---------------------------------------------------------------------------

// resolver tracks which flags the user explicitly set, so unset flags
// don't shadow legitimate env values.
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
	return parseIntEnv(env, def)
}

func parseIntEnv(env string, def int) (int, error) {
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer (got %q)", env, v)
	}
	return n, nil
}

func (r *resolver) boolVal(flag, env string, def bool) (bool, error) {
	if r.setFlags[flag] {
		return def, nil
	}
	return parseBoolEnv(env, def)
}

func parseBoolEnv(env string, def bool) (bool, error) {
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	b, ok := boolValues[strings.ToLower(v)]
	if !ok {
		return false, fmt.Errorf("%s must be a boolean (got %q)", env, v)
	}
	return b, nil
}

// boolValues is the set of accepted boolean string forms. Lookup is
// O(1) and keeps parseBoolEnv at low cyclomatic complexity.
var boolValues = map[string]bool{
	"1": true, "yes": true, "true": true,
	"0": false, "no": false, "false": false,
}

func (r *resolver) duration(flag, env string, def int) (time.Duration, error) {
	n, err := r.intVal(flag, env, def)
	if err != nil {
		return 0, err
	}
	return secondsToDuration(env, n)
}

// ---------------------------------------------------------------------------
// Auth-token resolution
// ---------------------------------------------------------------------------

// loadAuthToken resolves the auth token following the precedence:
//
//	--auth-token > --auth-token-file > DFLOCKD_AUTH_TOKEN > DFLOCKD_AUTH_TOKEN_FILE
func (r *resolver) loadAuthToken(flagToken, flagTokenFile string) (string, error) {
	if v, ok, err := r.flagAuthToken(flagToken); ok {
		return v, err
	}
	if v, ok, err := r.flagAuthTokenFile(flagTokenFile); ok {
		return v, err
	}
	return envAuthToken()
}

func (r *resolver) flagAuthToken(flagToken string) (string, bool, error) {
	if !r.setFlags["auth-token"] || flagToken == "" {
		return "", false, nil
	}
	v, err := cleanAuthToken("--auth-token", flagToken)
	return v, true, err
}

func (r *resolver) flagAuthTokenFile(flagTokenFile string) (string, bool, error) {
	if !r.setFlags["auth-token-file"] || flagTokenFile == "" {
		return "", false, nil
	}
	v, err := readAuthTokenFile(flagTokenFile)
	return v, true, err
}

func envAuthToken() (string, error) {
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

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

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
	return nonEmptyTrimmed(strings.Split(value, ","))
}

// nonEmptyTrimmed trims each input and drops empty results.
func nonEmptyTrimmed(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
