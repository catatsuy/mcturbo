package cluster

import (
	"errors"
	"hash/crc32"
	"hash/fnv"
	"time"

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
	removeFailedServers bool
	serverFailureLimit  int
	retryTimeout        time.Duration
	factory             shardFactory
	routerFactory       RouterFactory
}

func defaultClusterConfig() clusterConfig {
	return clusterConfig{
		vnodeFactor:  defaultVnodeFactor,
		distribution: DistributionModula,
		hash:         HashDefault,
		// libmemcached-like defaults for auto-eject behavior.
		serverFailureLimit: 2,
		retryTimeout:       2 * time.Second,
		factory: func(addr string, opts ...mcturbo.Option) (shardClient, error) {
			return mcturbo.New(addr, opts...)
		},
		routerFactory: DefaultRouterFactory,
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

// WithRemoveFailedServers enables temporary auto-eject on communication failures.
func WithRemoveFailedServers(enabled bool) ClusterOption {
	return func(c *clusterConfig) error {
		c.removeFailedServers = enabled
		return nil
	}
}

// WithServerFailureLimit sets failures before a server is temporarily ejected.
func WithServerFailureLimit(n int) ClusterOption {
	return func(c *clusterConfig) error {
		if n <= 0 {
			return errors.New("cluster: server failure limit must be > 0")
		}
		c.serverFailureLimit = n
		return nil
	}
}

// WithRetryTimeout sets how long an ejected server stays out of the ring.
func WithRetryTimeout(d time.Duration) ClusterOption {
	return func(c *clusterConfig) error {
		if d <= 0 {
			return errors.New("cluster: retry timeout must be > 0")
		}
		c.retryTimeout = d
		return nil
	}
}

// WithRouterFactory sets a custom router builder used by NewCluster and UpdateServers.
func WithRouterFactory(f RouterFactory) ClusterOption {
	return func(c *clusterConfig) error {
		if f == nil {
			return errors.New("cluster: router factory is nil")
		}
		c.routerFactory = f
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
