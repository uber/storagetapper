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

var cache map[string]Pipe
var lock sync.Mutex

// CacheGet returns an instance of pipe with specified config from cache or
// creates new one if it's not in the cache yet
func CacheGet(pipeType string, cfg *config.PipeConfig, db *sql.DB) (Pipe, error) {
	lock.Lock()
	defer lock.Unlock()

	if cache == nil {
		cache = make(map[string]Pipe)
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	_, _ = h.Write([]byte(pipeType + "$$$" + fmt.Sprintf("%p", db) + "$$$"))
	_, _ = h.Write(b)
	hs := fmt.Sprintf("%0x", h.Sum(nil))

	p := cache[hs]
	if p != nil && reflect.DeepEqual(cfg, p.Config()) {
		return p, nil
	}

	//FIXME: Implement proper collisions handling

	p, err = Create(pipeType, cfg, db)
	if err != nil {
		return nil, err
	}

	cache[hs] = p

	log.Debugf("Created and cached new '%v' pipe (hash %v) with config: %+v. Cache size %v", pipeType, hs, *cfg, len(cache))

	return p, nil
}

// CacheDestroy releases all resources associated with cached pipes
func CacheDestroy() {
	lock.Lock()
	defer lock.Unlock()

	for h, p := range cache {
		log.Debugf("Closing %v pipe (hash %v) with config %+v", p.Type(), h, p.Config())
		log.E(p.Close())
	}

	cache = nil
}
