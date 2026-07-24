// Package config loads dflockd configuration from CLI flags and env vars.
//
// Precedence: explicit CLI flag > environment variable > flag default.
package config

import (
	"flag"
	"fmt"
	"math"
	"net"
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
	FenceStateFile     string
	// OrphanTTL bounds how long a stable-ref waiter/holder survives in
	// the FSM after its TCP connection went away. Zero means today's
	// strict behavior (CleanupConn removes immediately). Must be the
	// same on every cluster member — it's read in the FSM apply path.
	OrphanTTL time.Duration

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

	// Cluster (Raft HA). Empty RaftDir leaves cluster mode off; everything
	// else is dormant in that case and the single-node behaviour holds.
	RaftDir          string
	NodeID           string
	ClusterPeers     []ClusterPeer // every member (including this node)
	RaftAddr         string        // this node's Raft transport bind ("host:port")
	AdvertiseAddr    string        // this node's client-facing host:port (returned to redirected clients)
	ClusterBootstrap bool          // bootstrap a new cluster vs. join an existing one
	// RaftAuthToken is required in cluster mode. It authenticates the
	// challenge-response handshake and derives per-connection AEAD keys.
	// Every member must use the same high-entropy value.
	RaftAuthToken string
	// Mutual TLS on the Raft transport. All three set → every inter-node
	// connection is mTLS (each node presents RaftTLSCert and verifies the
	// peer against RaftTLSCA). Certificate Common Names must equal NodeID.
	// All empty leaves the shared-secret AEAD as the transport protection.
	RaftTLSCert string
	RaftTLSKey  string
	RaftTLSCA   string
	// AdminToken gates the cluster-reconfig HTTP endpoints
	// (POST/DELETE /v1/admin/voters). Empty → admin endpoints return
	// 503 admin_disabled (default-deny). Sourced from --admin-token or
	// DFLOCKD_ADMIN_TOKEN. Compared in constant time.
	AdminToken string

	// Diagnostics.
	Debug   bool
	Version bool
}

// ClusterPeer is one member of the cluster.
type ClusterPeer struct {
	NodeID     string // stable raft node id
	RaftAddr   string // raft transport address ("host:port")
	ClientAddr string // client-facing address ("host:port")
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
	host, tlsCert, tlsKey                           *string
	authToken, authTokenFile                        *string
	raftAuthToken, raftAuthTokenFile                *string
	adminToken                                      *string
	httpHost, httpCORSOrigins                       *string
	fenceStateFile                                  *string
	raftDir, nodeID, clusterPeers                   *string
	raftAddr, advertiseAddr                         *string
	raftTLSCert, raftTLSKey, raftTLSCA              *string
	port, maxLocks                                  *int
	maxConnections, maxConnectionsPerIP, maxWaiters *int
	defaultLeaseTTL, leaseSweepInterval             *int
	gcInterval, gcMaxIdle, orphanTTL                *int
	readTimeout, writeTimeout, shutdownTimeout      *int
	httpPort, httpIdle                              *int
	httpMaxSessions, httpMaxSessionsPerIP           *int
	httpMaxConnsPerIP                               *int
	httpRateLimitPerIP, httpRateLimitBurst          *int
	autoRelease, debug, version, clusterBootstrap   *bool
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
	f.adminToken = fs.String("admin-token", "", "Admin secret token for cluster reconfig (POST/DELETE /v1/admin/voters). Empty = admin endpoints return 503 admin_disabled. Prefer DFLOCKD_ADMIN_TOKEN env to avoid process-list exposure.")
	f.httpHost = fs.String("http-host", "", "HTTP API bind address (defaults to --host)")
	f.httpCORSOrigins = fs.String("http-cors-allowed-origins", "", "Comma-separated allowed CORS origins for the HTTP API (empty = disabled)")
	f.fenceStateFile = fs.String("fence-state-file", "", "Path to the fence-counter state file. Set to enable strict cross-restart fencing-token monotonicity (one fsync per ~1M grants). Empty = best-effort wall-clock seeding.")
	f.raftDir = fs.String("raft-dir", "", "Directory for Raft persistent state (log, snapshots, hardstate). Empty = cluster mode disabled.")
	f.nodeID = fs.String("node-id", "", "This node's stable cluster identifier (required in cluster mode).")
	f.clusterPeers = fs.String("cluster-peers", "", "Comma-separated list of cluster members: id=raftHost:raftPort@clientHost:clientPort. Required in cluster mode; must include this node.")
	f.raftAddr = fs.String("raft-addr", "", "This node's Raft transport bind address (host:port). Required in cluster mode.")
	f.advertiseAddr = fs.String("advertise-addr", "", "This node's client-facing host:port returned to clients via error_not_leader. Defaults to --host:--port.")
	f.raftAuthToken = fs.String("raft-auth-token", "", "Shared secret for Raft peer authentication and encryption (minimum 32 bytes; prefer --raft-auth-token-file).")
	f.raftAuthTokenFile = fs.String("raft-auth-token-file", "", "Path to a file containing the shared Raft auth token.")
	f.raftTLSCert = fs.String("raft-tls-cert", "", "PEM cert for mutual TLS on the Raft transport. Set with --raft-tls-key and --raft-tls-ca to encrypt+authenticate all inter-node traffic.")
	f.raftTLSKey = fs.String("raft-tls-key", "", "PEM private key for --raft-tls-cert.")
	f.raftTLSCA = fs.String("raft-tls-ca", "", "PEM CA bundle used to verify Raft peers' certificates (enables mutual TLS when set with --raft-tls-cert/--raft-tls-key).")
}

func defineIntFlags(fs *flag.FlagSet, f *flagPtrs) {
	f.port = fs.Int("port", 6388, "Bind port")
	f.defaultLeaseTTL = fs.Int("default-lease-ttl", 33, "Default lock lease duration (seconds)")
	f.leaseSweepInterval = fs.Int("lease-sweep-interval", 1, "Lease expiry check interval (seconds)")
	f.gcInterval = fs.Int("gc-interval", 5, "Lock state GC interval (seconds)")
	f.gcMaxIdle = fs.Int("gc-max-idle", 60, "Idle seconds before pruning lock state")
	f.orphanTTL = fs.Int("orphan-ttl", 0, "Cluster only: seconds a stable-ref lock/queue slot survives in the FSM after its client's connection drops, before reclamation. Enables FIFO-preserving failover re-attach (reconnect with the same stable ref reclaims the slot). 0 = reclaim immediately. Must be identical on every cluster member.")
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
	f.clusterBootstrap = fs.Bool("cluster-bootstrap", false, "Bootstrap a fresh single-node cluster on this --raft-dir. The cluster grows via membership changes from there.")
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
	peers, err := parseClusterPeers(r.str("cluster-peers", "DFLOCKD_CLUSTER_PEERS", *f.clusterPeers))
	if err != nil {
		return err
	}
	c.ClusterPeers = peers
	return nil
}

// parseClusterPeers parses the cluster-peers spec
// "id=raftHost:raftPort@clientHost:clientPort,...". Empty input -> nil.
func parseClusterPeers(spec string) ([]ClusterPeer, error) {
	if spec == "" {
		return nil, nil
	}
	parts := strings.Split(spec, ",")
	out := make([]ClusterPeer, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		cp, err := parseOnePeer(p)
		if err != nil {
			return nil, fmt.Errorf("--cluster-peers[%d] %q: %w", i, p, err)
		}
		out = append(out, cp)
	}
	return out, nil
}

func parseOnePeer(s string) (ClusterPeer, error) {
	idAndRest, ok := strings.CutPrefix(s, "")
	if !ok {
		return ClusterPeer{}, fmt.Errorf("empty peer spec")
	}
	id, addrs, ok := strings.Cut(idAndRest, "=")
	if !ok || id == "" || addrs == "" {
		return ClusterPeer{}, fmt.Errorf("missing '=' between id and address")
	}
	raft, client, ok := strings.Cut(addrs, "@")
	if !ok || raft == "" || client == "" {
		return ClusterPeer{}, fmt.Errorf("missing '@' between raft-addr and client-addr")
	}
	return ClusterPeer{NodeID: id, RaftAddr: raft, ClientAddr: client}, nil
}

func stringResolvers(f *flagPtrs) []stringResolver {
	return []stringResolver{
		{"host", "DFLOCKD_HOST", f.host, func(c *Config, v string) { c.Host = v }},
		{"tls-cert", "DFLOCKD_TLS_CERT", f.tlsCert, func(c *Config, v string) { c.TLSCert = v }},
		{"tls-key", "DFLOCKD_TLS_KEY", f.tlsKey, func(c *Config, v string) { c.TLSKey = v }},
		{"http-host", "DFLOCKD_HTTP_HOST", f.httpHost, func(c *Config, v string) { c.HTTPHost = v }},
		{"fence-state-file", "DFLOCKD_FENCE_STATE_FILE", f.fenceStateFile, func(c *Config, v string) { c.FenceStateFile = v }},
		{"raft-dir", "DFLOCKD_RAFT_DIR", f.raftDir, func(c *Config, v string) { c.RaftDir = v }},
		{"node-id", "DFLOCKD_NODE_ID", f.nodeID, func(c *Config, v string) { c.NodeID = v }},
		{"raft-addr", "DFLOCKD_RAFT_ADDR", f.raftAddr, func(c *Config, v string) { c.RaftAddr = v }},
		{"advertise-addr", "DFLOCKD_ADVERTISE_ADDR", f.advertiseAddr, func(c *Config, v string) { c.AdvertiseAddr = v }},
		{"raft-tls-cert", "DFLOCKD_RAFT_TLS_CERT", f.raftTLSCert, func(c *Config, v string) { c.RaftTLSCert = v }},
		{"raft-tls-key", "DFLOCKD_RAFT_TLS_KEY", f.raftTLSKey, func(c *Config, v string) { c.RaftTLSKey = v }},
		{"raft-tls-ca", "DFLOCKD_RAFT_TLS_CA", f.raftTLSCA, func(c *Config, v string) { c.RaftTLSCA = v }},
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
		{"cluster-bootstrap", "DFLOCKD_CLUSTER_BOOTSTRAP", f.clusterBootstrap, func(c *Config, v bool) { c.ClusterBootstrap = v }},
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
		{"orphan-ttl", "DFLOCKD_ORPHAN_TTL_S", f.orphanTTL, func(c *Config, v time.Duration) { c.OrphanTTL = v }},
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
	raftToken, err := r.loadRaftAuthToken(*f.raftAuthToken, *f.raftAuthTokenFile)
	if err != nil {
		return err
	}
	c.RaftAuthToken = raftToken
	c.AdminToken = r.loadAdminToken(*f.adminToken)
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
	validateClusterFields,
	validateClusterVsFenceFile,
	validateRaftTLS,
	validateRaftAuthToken,
	validateOrphanTTL,
}

// validateOrphanTTL rejects a negative TTL and the no-op combination of
// a positive TTL outside cluster mode (the orphan/re-adopt machinery
// lives in the Raft FSM apply path, which single-node mode never runs —
// so a single-node --orphan-ttl would silently do nothing).
func validateOrphanTTL(c *Config) error {
	if c.OrphanTTL < 0 {
		return fmt.Errorf("--orphan-ttl must be >= 0")
	}
	if c.OrphanTTL > 0 && !c.IsCluster() {
		return fmt.Errorf("--orphan-ttl requires cluster mode (--raft-dir); it has no effect single-node")
	}
	return nil
}

// validateClusterFields enforces that cluster fields are coherent: if
// any of them is set, all required ones must be set.
func validateClusterFields(c *Config) error {
	if !c.IsCluster() {
		return nil
	}
	if c.NodeID == "" {
		return fmt.Errorf("--node-id is required in cluster mode")
	}
	if c.RaftAddr == "" {
		return fmt.Errorf("--raft-addr is required in cluster mode")
	}
	if len(c.ClusterPeers) == 0 {
		return fmt.Errorf("--cluster-peers must list at least this node")
	}
	if !peersIncludeNode(c.ClusterPeers, c.NodeID) {
		return fmt.Errorf("--cluster-peers must include this node's --node-id (%q)", c.NodeID)
	}
	return validateClusterPeerUniqueness(c.ClusterPeers)
}

func peersIncludeNode(peers []ClusterPeer, id string) bool {
	for _, p := range peers {
		if p.NodeID == id {
			return true
		}
	}
	return false
}

func validateClusterPeerUniqueness(peers []ClusterPeer) error {
	seen := map[string]bool{}
	for _, p := range peers {
		if seen[p.NodeID] {
			return fmt.Errorf("--cluster-peers: duplicate node id %q", p.NodeID)
		}
		seen[p.NodeID] = true
	}
	return nil
}

// validateClusterVsFenceFile rejects the combination of --fence-state-file
// with cluster mode (Raft persistence supersedes it; running both would
// duplicate fences inconsistently across nodes).
func validateClusterVsFenceFile(c *Config) error {
	if c.IsCluster() && c.FenceStateFile != "" {
		return fmt.Errorf("--fence-state-file is incompatible with cluster mode; Raft persistence supersedes it")
	}
	return nil
}

// validateRaftTLS enforces the all-three-or-none rule on the Raft mTLS
// flags and ties them to cluster mode.
func validateRaftTLS(c *Config) error {
	set := 0
	for _, v := range []string{c.RaftTLSCert, c.RaftTLSKey, c.RaftTLSCA} {
		if v != "" {
			set++
		}
	}
	if set == 0 {
		return nil
	}
	if set != 3 {
		return fmt.Errorf("--raft-tls-cert, --raft-tls-key and --raft-tls-ca must be set together (or all left empty)")
	}
	if !c.IsCluster() {
		return fmt.Errorf("--raft-tls-* requires cluster mode (--raft-dir)")
	}
	return nil
}

// RaftTLSEnabled reports whether mutual TLS is configured for the Raft
// transport.
func (c *Config) RaftTLSEnabled() bool { return c.RaftTLSCert != "" }

func validateRaftAuthToken(c *Config) error {
	if !c.IsCluster() {
		if c.RaftAuthToken != "" {
			return fmt.Errorf("--raft-auth-token requires cluster mode (--raft-dir)")
		}
		return nil
	}
	if len(c.RaftAuthToken) < 32 {
		return fmt.Errorf("--raft-auth-token is required in cluster mode and must be at least 32 bytes")
	}
	return nil
}

// IsCluster reports whether cluster mode is enabled. We use --raft-dir
// as the canonical "are we clustered?" switch: it is the only required
// flag whose presence has no useful single-node interpretation.
func (c *Config) IsCluster() bool { return c.RaftDir != "" }

// EffectiveAdvertiseAddr returns the address a redirected client should
// retry against. Falls back to host:port when --advertise-addr is unset.
func (c *Config) EffectiveAdvertiseAddr() string {
	if c.AdvertiseAddr != "" {
		return c.AdvertiseAddr
	}
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
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
// Operational posture warnings
// ---------------------------------------------------------------------------

// UnboundedLimitWarnings returns the names of resource-protection limits
// that are left unbounded, so cmd/dflockd can warn at startup. It stays
// quiet when the server is bound only to loopback (the permissive
// defaults are fine for local development).
func (c *Config) UnboundedLimitWarnings() []string {
	if c.boundToLoopback() {
		return nil
	}
	var w []string
	if c.MaxConnections == 0 {
		w = append(w, "--max-connections")
	}
	if c.MaxConnectionsPerIP == 0 {
		w = append(w, "--max-connections-per-ip")
	}
	if c.MaxWaiters == 0 {
		w = append(w, "--max-waiters")
	}
	if c.HTTPPort != 0 {
		w = appendHTTPLimitWarnings(w, c)
	}
	return w
}

func appendHTTPLimitWarnings(w []string, c *Config) []string {
	if c.HTTPMaxSessions == 0 {
		w = append(w, "--http-max-sessions")
	}
	if c.HTTPMaxSessionsPerIP == 0 {
		w = append(w, "--http-max-sessions-per-ip")
	}
	if c.HTTPMaxConnectionsPerIP == 0 {
		w = append(w, "--http-max-connections-per-ip")
	}
	if c.HTTPRateLimitPerIP == 0 {
		w = append(w, "--http-rate-limit-per-ip")
	}
	return w
}

// boundToLoopback reports whether every bind address is loopback. Only
// the literal forms are recognised; a hostname or "0.0.0.0" is treated
// as potentially reachable.
func (c *Config) boundToLoopback() bool {
	if !isLoopbackHost(c.Host) {
		return false
	}
	return c.HTTPPort == 0 || isLoopbackHost(httpBindHost(c))
}

func httpBindHost(c *Config) string {
	if c.HTTPHost != "" {
		return c.HTTPHost
	}
	return c.Host
}

func isLoopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
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

// loadRaftAuthToken uses the same secret-source precedence as the client
// auth token, but its own flags and environment variables.
func (r *resolver) loadRaftAuthToken(flagToken, flagTokenFile string) (string, error) {
	if r.setFlags["raft-auth-token"] && flagToken != "" {
		return cleanAuthToken("--raft-auth-token", flagToken)
	}
	if r.setFlags["raft-auth-token-file"] && flagTokenFile != "" {
		return readAuthTokenFile(flagTokenFile)
	}
	if v := os.Getenv("DFLOCKD_RAFT_AUTH_TOKEN"); v != "" {
		return cleanAuthToken("DFLOCKD_RAFT_AUTH_TOKEN", v)
	}
	if v := os.Getenv("DFLOCKD_RAFT_AUTH_TOKEN_FILE"); v != "" {
		return readAuthTokenFile(v)
	}
	return "", nil
}

// loadAdminToken resolves the admin token: --admin-token > DFLOCKD_ADMIN_TOKEN.
// Whitespace is trimmed; an admin token containing newlines is treated
// as unset (admin endpoints stay default-deny rather than admitting a
// malformed secret). Errors from cleaning are dropped intentionally —
// the conservative choice is "admin disabled" not "startup fails".
func (r *resolver) loadAdminToken(flagToken string) string {
	if r.setFlags["admin-token"] && flagToken != "" {
		if v, err := cleanAuthToken("--admin-token", flagToken); err == nil {
			return v
		}
		return ""
	}
	if v := os.Getenv("DFLOCKD_ADMIN_TOKEN"); v != "" {
		if v, err := cleanAuthToken("DFLOCKD_ADMIN_TOKEN", v); err == nil {
			return v
		}
	}
	return ""
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
