package cluster

import (
	"errors"
	"hash/crc32"
	"hash/fnv"

	"github.com/catatsuy/mcturbo"
)

// Distribution controls key-to-shard routing strategy.
type Distribution uint8

const (
	// DistributionDefault uses the package default distribution.
	DistributionDefault Distribution = iota
	// DistributionModula maps keys by hash(key) % serverCount.
	DistributionModula
	// DistributionConsistent maps keys with a consistent-hash ring.
	DistributionConsistent
)

// Hash controls key hashing algorithm.
type Hash uint8

const (
	// HashDefault uses the package default hash.
	HashDefault Hash = iota
	// HashMD5 uses MD5-based hashing.
	HashMD5
	// HashCRC32 uses CRC32 hashing.
	HashCRC32
)

// ClusterOption configures Cluster behavior.
type ClusterOption func(*clusterConfig) error

type clusterConfig struct {
	baseClientOptions   []mcturbo.Option
	vnodeFactor         int
	distribution        Distribution
	hash                Hash
	libketamaCompatible bool
	factory             shardFactory
}

func defaultClusterConfig() clusterConfig {
	return clusterConfig{
		vnodeFactor:  defaultVnodeFactor,
		distribution: DistributionModula,
		hash:         HashDefault,
		factory: func(addr string, opts ...mcturbo.Option) (shardClient, error) {
			return mcturbo.New(addr, opts...)
		},
	}
}

// WithBaseClientOptions applies base mcturbo options to all shard clients.
func WithBaseClientOptions(opts ...mcturbo.Option) ClusterOption {
	return func(c *clusterConfig) error {
		c.baseClientOptions = append([]mcturbo.Option(nil), opts...)
		return nil
	}
}

// WithVnodeFactor sets virtual-node factor for consistent hashing.
func WithVnodeFactor(n int) ClusterOption {
	return func(c *clusterConfig) error {
		if n <= 0 {
			return errors.New("cluster: vnode factor must be > 0")
		}
		c.vnodeFactor = n
		return nil
	}
}

// WithDistribution sets key distribution strategy.
func WithDistribution(d Distribution) ClusterOption {
	return func(c *clusterConfig) error {
		switch d {
		case DistributionDefault, DistributionModula, DistributionConsistent:
			c.distribution = d
			return nil
		default:
			return errors.New("cluster: invalid distribution")
		}
	}
}

// WithHash sets key hash algorithm.
func WithHash(h Hash) ClusterOption {
	return func(c *clusterConfig) error {
		switch h {
		case HashDefault, HashMD5, HashCRC32:
			c.hash = h
			return nil
		default:
			return errors.New("cluster: invalid hash")
		}
	}
}

// WithLibketamaCompatible enables libketama-compatible behavior.
// When enabled, distribution is forced to consistent and hash is forced to MD5.
func WithLibketamaCompatible(enabled bool) ClusterOption {
	return func(c *clusterConfig) error {
		c.libketamaCompatible = enabled
		return nil
	}
}

func effectiveDistribution(c *clusterConfig) Distribution {
	if c.libketamaCompatible {
		return DistributionConsistent
	}
	if c.distribution == DistributionDefault {
		return DistributionModula
	}
	return c.distribution
}

func effectiveHash(c *clusterConfig) Hash {
	if c.libketamaCompatible {
		return HashMD5
	}
	if c.hash == HashDefault {
		return HashDefault
	}
	return c.hash
}

type hashFunc func(string) uint32

func resolveHash(h Hash) hashFunc {
	switch h {
	case HashMD5:
		return hashMD5Uint32
	case HashCRC32:
		return func(s string) uint32 {
			return crc32.ChecksumIEEE([]byte(s))
		}
	case HashDefault:
		fallthrough
	default:
		return func(s string) uint32 {
			hh := fnv.New32a()
			_, _ = hh.Write([]byte(s))
			return hh.Sum32()
		}
	}
}
