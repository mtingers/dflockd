package raft

import "fmt"

// configurationAt returns the configuration effective at index. The live
// configuration is a fast path unless it comes from a later, uncommitted log
// entry; that case is why snapshots cannot simply use n.config.
//
// Both paths return the same canonical form. n.config has been through
// withBootstrapClientMetadata (adoptConfig applies it), so without the same
// treatment here two replicas at identical log state could stamp a snapshot at
// the same index with and without client metadata — which diverges snapshot
// bytes and makes writePreparedSnapshot reject a regenerated generation as
// having conflicting metadata.
func (n *Node) configurationAt(index Index) (Configuration, Index, error) {
	if n.cfgIndex <= index {
		return n.config.Clone(), n.cfgIndex, nil
	}
	cfg, idx, err := n.loadConfigurationAt(index)
	if err != nil {
		return Configuration{}, 0, err
	}
	return n.withBootstrapClientMetadata(cfg), idx, nil
}

// loadConfigurationAt reconstructs membership from durable state. The latest
// EntryConfig at or before index wins, followed by the snapshot configuration,
// then the immutable bootstrap configuration.
func (n *Node) loadConfigurationAt(index Index) (Configuration, Index, error) {
	if last := n.log.lastIndex(); index > last {
		index = last
	}
	first := n.log.firstIndex()
	if index >= first {
		for i := index; ; i-- {
			entries, err := n.log.entries(i, i+1)
			if err != nil {
				return Configuration{}, 0, fmt.Errorf("raft: read config at %d: %w", i, err)
			}
			if len(entries) != 1 {
				return Configuration{}, 0, fmt.Errorf("raft: missing config candidate at %d", i)
			}
			if entries[0].Type == EntryConfig {
				cfg, err := decodeConfig(entries[0].Data)
				if err != nil {
					return Configuration{}, 0, fmt.Errorf("raft: decode persisted config at %d: %w", i, err)
				}
				return cfg, i, nil
			}
			if i == first {
				break
			}
		}
	}
	if meta, ok := n.log.storage.SnapshotMeta(); ok &&
		meta.LastIncludedIndex <= index &&
		len(meta.Configuration.Voters) > 0 {
		return meta.Configuration.Clone(), meta.LastIncludedIndex, nil
	}
	return n.bootstrapConfig.Clone(), 0, nil
}

// reconcileConfiguration restores the effective configuration after a leader
// append has replaced a suffix that may have contained EntryConfig records.
func (n *Node) reconcileConfiguration() error {
	cfg, idx, err := n.loadConfigurationAt(n.log.lastIndex())
	if err != nil {
		return err
	}
	n.adoptConfig(cfg, idx)
	return nil
}
