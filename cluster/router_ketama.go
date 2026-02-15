package cluster

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"strconv"
)

const defaultVnodeFactor = 40

type ketamaPoint struct {
	hash  uint32
	shard int
}

type ketamaRouter struct {
	hashes []uint32
	shards []int
	hash   hashFunc
}

func newKetamaRouter(servers []Server, hashType Hash, hf hashFunc, vnodeFactor int) *ketamaRouter {
	if vnodeFactor <= 0 {
		vnodeFactor = defaultVnodeFactor
	}
	points := make([]ketamaPoint, 0, len(servers)*vnodeFactor*4)
	for shardIdx := range servers {
		s := servers[shardIdx]
		vnodes := vnodeFactor * s.Weight
		for i := range vnodes {
			token := s.Addr + "-" + strconv.Itoa(i)
			if hashType == HashMD5 {
				sum := md5.Sum([]byte(token))
				points = append(points,
					ketamaPoint{hash: binary.LittleEndian.Uint32(sum[0:4]), shard: shardIdx},
					ketamaPoint{hash: binary.LittleEndian.Uint32(sum[4:8]), shard: shardIdx},
					ketamaPoint{hash: binary.LittleEndian.Uint32(sum[8:12]), shard: shardIdx},
					ketamaPoint{hash: binary.LittleEndian.Uint32(sum[12:16]), shard: shardIdx},
				)
				continue
			}
			for j := 0; j < 4; j++ {
				h := hf(token + "#" + strconv.Itoa(j))
				points = append(points, ketamaPoint{hash: h, shard: shardIdx})
			}
		}
	}

	sort.Slice(points, func(i, j int) bool {
		if points[i].hash == points[j].hash {
			return points[i].shard < points[j].shard
		}
		return points[i].hash < points[j].hash
	})

	r := &ketamaRouter{
		hashes: make([]uint32, len(points)),
		shards: make([]int, len(points)),
		hash:   hf,
	}
	for i := range points {
		r.hashes[i] = points[i].hash
		r.shards[i] = points[i].shard
	}
	return r
}

func (r *ketamaRouter) Pick(key string) int {
	if len(r.hashes) == 0 {
		return -1
	}
	h := r.hash(key)
	i := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})
	if i == len(r.hashes) {
		i = 0
	}
	return r.shards[i]
}

func hashMD5Uint32(key string) uint32 {
	sum := md5.Sum([]byte(key))
	return binary.LittleEndian.Uint32(sum[0:4])
}
