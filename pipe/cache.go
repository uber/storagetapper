package pipe

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/uber/storagetapper/config"
	"github.com/uber/storagetapper/log"
)

type cacheEntry struct {
	pipe Pipe
	cfg  config.PipeConfig
}

var cache map[string]cacheEntry
var lock sync.Mutex

// CacheGet returns an instance of pipe with specified config from cache or
// creates new one if it's not in the cache yet
func CacheGet(pipeType string, cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	lock.Lock()
	defer lock.Unlock()

	if cache == nil {
		cache = make(map[string]cacheEntry)
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	_, _ = h.Write([]byte(pipeType + "$$$" + fmt.Sprintf("%p", db) + "$$$"))
	_, _ = h.Write(b)
	hs := fmt.Sprintf("%0x", h.Sum(nil))

	p, ok := cache[hs]
	if ok && reflect.DeepEqual(cfg, &p.cfg) {
		return p.pipe, nil
	}

	//FIXME: Implement proper collisions handling

	np, err := Create(pipeType, cfg, db)
	if err != nil {
		return nil, err
	}

	cache[hs] = cacheEntry{np, *cfg}

	log.Debugf("Created and cached new '%v' pipe (hash %v) with config: %+v. Cache size %v", pipeType, hs, *cfg, len(cache))

	return np, nil
}

// CacheDestroy releases all resources associated with cached pipes
func CacheDestroy() {
	lock.Lock()
	defer lock.Unlock()

	for h, p := range cache {
		log.Debugf("Closing %v pipe (hash %v) with config %+v", p.pipe.Type(), h, p.cfg)
		log.E(p.pipe.Close())
	}

	cache = nil
}
