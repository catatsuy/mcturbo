package cluster

import "errors"

// Router decides shard index from a key.
type Router interface {
	Pick(key string) int
}

var errNoServers = errors.New("cluster: no servers")

func newRouter(servers []Server, distribution Distribution, hash Hash, vnodeFactor int) (Router, error) {
	if len(servers) == 0 {
		return nil, errNoServers
	}
	hf := resolveHash(hash)
	switch distribution {
	case DistributionModula:
		return newModulaRouter(len(servers), hf), nil
	case DistributionConsistent:
		return newKetamaRouter(servers, hash, hf, vnodeFactor), nil
	default:
		return nil, errors.New("cluster: invalid distribution")
	}
}
