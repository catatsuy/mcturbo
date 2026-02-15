package cluster

type modulaRouter struct {
	count int
	hash  hashFunc
}

func newModulaRouter(count int, hf hashFunc) *modulaRouter {
	return &modulaRouter{count: count, hash: hf}
}

func (r *modulaRouter) Pick(key string) int {
	if r.count <= 0 {
		return -1
	}
	h := r.hash(key)
	return int(h % uint32(r.count))
}
