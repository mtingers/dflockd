package config

import (
	"strings"
	"testing"
)

func TestParseClusterPeers(t *testing.T) {
	cases := map[string]struct {
		want    []ClusterPeer
		wantErr string
	}{
		"":                          {nil, ""},
		"n1=h1:1@c1:2":              {[]ClusterPeer{{"n1", "h1:1", "c1:2"}}, ""},
		"n1=h1:1@c1:2,n2=h2:1@c2:2": {[]ClusterPeer{{"n1", "h1:1", "c1:2"}, {"n2", "h2:1", "c2:2"}}, ""},
		"bad":                       {nil, "missing '='"},
		"id=raftonly":               {nil, "missing '@'"},
		"=raft@client":              {nil, "missing '='"},
	}
	for input, want := range cases {
		t.Run("in="+input, func(t *testing.T) {
			got, err := parseClusterPeers(input)
			if want.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), want.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, want.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !sameClusterPeers(got, want.want) {
				t.Fatalf("got %+v, want %+v", got, want.want)
			}
		})
	}
}

func sameClusterPeers(a, b []ClusterPeer) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIsClusterAndEffectiveAdvertise(t *testing.T) {
	c := &Config{Host: "10.0.0.1", Port: 6388}
	if c.IsCluster() {
		t.Fatalf("empty raft-dir should not be cluster mode")
	}
	if got := c.EffectiveAdvertiseAddr(); got != "10.0.0.1:6388" {
		t.Fatalf("derived advertise = %q", got)
	}
	c.AdvertiseAddr = "external:9000"
	if got := c.EffectiveAdvertiseAddr(); got != "external:9000" {
		t.Fatalf("explicit advertise = %q", got)
	}
	c.RaftDir = "/tmp/x"
	if !c.IsCluster() {
		t.Fatalf("raft-dir set should be cluster mode")
	}
}

func TestClusterFieldValidation(t *testing.T) {
	base := func() *Config {
		c, err := Load([]string{
			"--raft-dir", "/tmp/raft",
			"--node-id", "n1",
			"--raft-addr", "127.0.0.1:7001",
			"--cluster-peers", "n1=127.0.0.1:7001@127.0.0.1:6388,n2=127.0.0.1:7002@127.0.0.1:6389",
		})
		if err != nil {
			t.Fatalf("load valid cluster cfg: %v", err)
		}
		return c
	}
	if c := base(); !c.IsCluster() || len(c.ClusterPeers) != 2 || c.NodeID != "n1" {
		t.Fatalf("base cfg unexpected: %+v", c)
	}

	bads := []struct {
		args []string
		want string
	}{
		{[]string{"--raft-dir", "/d"}, "--node-id"},
		{[]string{"--raft-dir", "/d", "--node-id", "n1"}, "--raft-addr"},
		{[]string{"--raft-dir", "/d", "--node-id", "n1", "--raft-addr", ":7001"}, "--cluster-peers"},
		{[]string{"--raft-dir", "/d", "--node-id", "n1", "--raft-addr", ":7001", "--cluster-peers", "n2=h:1@c:1"}, "must include this node"},
		{[]string{"--raft-dir", "/d", "--node-id", "n1", "--raft-addr", ":7001", "--cluster-peers", "n1=h:1@c:1,n1=h:2@c:2"}, "duplicate node id"},
		{[]string{"--raft-dir", "/d", "--node-id", "n1", "--raft-addr", ":7001", "--cluster-peers", "n1=h:1@c:1", "--fence-state-file", "/fence"}, "--fence-state-file is incompatible"},
	}
	for _, bad := range bads {
		t.Run(strings.Join(bad.args, " "), func(t *testing.T) {
			_, err := Load(bad.args)
			if err == nil || !strings.Contains(err.Error(), bad.want) {
				t.Fatalf("want error containing %q, got %v", bad.want, err)
			}
		})
	}
}
