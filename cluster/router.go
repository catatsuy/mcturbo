package cluster

import "errors"

// Router decides shard index from a key.
type Router interface {
	Pick(key string) int
}

// RouterFactory builds a router from server list and routing settings.
type RouterFactory func(servers []Server, distribution Distribution, hash Hash, vnodeFactor int) (Router, error)

var errNoServers = errors.New("cluster: no servers")

// DefaultRouterFactory is the built-in router builder used when no custom factory is set.
func DefaultRouterFactory(servers []Server, distribution Distribution, hash Hash, vnodeFactor int) (Router, error) {
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

func newRouter(servers []Server, distribution Distribution, hash Hash, vnodeFactor int) (Router, error) {
	return DefaultRouterFactory(servers, distribution, hash, vnodeFactor)
}

// ModulaRouterFactory returns a built-in RouterFactory fixed to modula distribution.
func ModulaRouterFactory(hash Hash) RouterFactory {
	return func(servers []Server, _ Distribution, _ Hash, _ int) (Router, error) {
		if len(servers) == 0 {
			return nil, errNoServers
		}
		return newModulaRouter(len(servers), resolveHash(hash)), nil
	}
}

// ConsistentRouterFactory returns a built-in RouterFactory fixed to consistent distribution.
func ConsistentRouterFactory(hash Hash, vnodeFactor int) RouterFactory {
	return func(servers []Server, _ Distribution, _ Hash, _ int) (Router, error) {
		if len(servers) == 0 {
			return nil, errNoServers
		}
		return newKetamaRouter(servers, hash, resolveHash(hash), vnodeFactor), nil
	}
}
