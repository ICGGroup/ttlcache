package ttlcache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

const (
	groupsBucketName string = "__groups"
	defaultGroupName string = "default"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(map[string]string{})
	gob.Register([]interface{}{})
}

type TTLCache struct {
	Groups      map[string]*Group
	db          *bolt.DB
	storagePath string
	ticker      *time.Ticker
	stop        chan bool
}

func OpenCache(storagePath string, reapInterval time.Duration) (*TTLCache, error) {

	// TODO: jdouglas For now, deleting existing database on restart Mon Nov  7 09:31:03 2016
	os.RemoveAll(storagePath)

	// Open the database
	db, err := bolt.Open(storagePath, 0755, nil)
	if err != nil {
		return nil, err
	}

	if reapInterval < time.Millisecond {
		reapInterval = time.Millisecond
	}

	ttlc := TTLCache{
		db:          db,
		storagePath: storagePath,
	}

	ttlc.Groups = make(map[string]*Group)

	// Load up existing groups
	err = db.Update(func(tx *bolt.Tx) error {

		// Ensure the groups bucket exists
		gps, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
		if err != nil {
			return err
		}

		// Get any existing groups
		err = gps.ForEach(func(k, v []byte) error {
			g := Group{
				Key:      k,
				ttlIndex: []IndexEntry{},
				ttlCache: &ttlc,
			}

			gbkt, err := tx.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}

			g.Lock()
			defer g.Unlock()

			g.ttlIndex = []IndexEntry{}

			err = gbkt.ForEach(func(k, v []byte) error {

				e := CacheEntry{}

				r := bytes.NewBuffer(v)
				decoder := gob.NewDecoder(r)
				err := decoder.Decode(&e)
				if err != nil {
					return err
				}

				g.ttlIndex = append(g.ttlIndex, IndexEntry{Key: k, ExpiresAt: e.ExpiresAt})

				return nil

			})
			if err != nil {
				return err
			}

			sort.Sort(ByIndexEntryExpiry(g.ttlIndex))

			ttlc.Groups[string(k)] = &g
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	ttlc.ticker = time.NewTicker(reapInterval)
	ttlc.stop = make(chan bool, 1)

	go func() {
		for {
			select {
			case <-ttlc.ticker.C:
				ttlc.reapExpired()
			case <-ttlc.stop:
				return
			}
		}
	}()

	return &ttlc, nil

}

func (t *TTLCache) reapExpired() {
	// Loop over each group
	for _, g := range t.Groups {
		go func(g *Group) {

			g.ttlCache.db.Update(func(tx *bolt.Tx) error {
				now := time.Now()
				newIndex := []IndexEntry{}

				g.Lock()
				defer g.Unlock()

				for _, e := range g.ttlIndex {
					if e.ExpiresAt.Before(now) {

						gbkt := tx.Bucket(g.Key)
						if gbkt == nil {
							return fmt.Errorf("Bucket does not exist")
						}
						err := gbkt.Delete(e.Key)
						if err != nil {
							return err
						}

					} else {

						newIndex = append(newIndex, e)
					}
				}
				// Ensure the groups bucket exists

				g.ttlIndex = newIndex

				return nil

			})

		}(g)
	}
}

type Group struct {
	sync.Mutex
	Key      []byte
	ttlIndex []IndexEntry
	ttlCache *TTLCache
}

func (g *Group) Count() int {
	return len(g.ttlIndex)
}

func (g *Group) Put(key []byte, value interface{}, ttl time.Duration) error {

	err := g.ttlCache.db.Update(func(tx *bolt.Tx) error {

		// Ensure the groups bucket exists
		gbkt, err := tx.CreateBucketIfNotExists(g.Key)
		if err != nil {
			return err
		}

		existing := gbkt.Get(key)
		if existing != nil {
			// We are doing this primarily to ensure that the index stays in sync with the entries
			gbkt.Delete(key)
		}

		b := new(bytes.Buffer)
		now := time.Now()

		e := CacheEntry{
			Key:       key,
			Value:     value,
			CreatedAt: now,
			ExpiresAt: now.Add(ttl),
		}

		encoder := gob.NewEncoder(b)
		err = encoder.Encode(&e)
		if err != nil {
			return err
		}

		// Add the item to the group bucket
		err = gbkt.Put(key, b.Bytes())
		if err != nil {
			return err
		}

		g.Lock()
		defer g.Unlock()

		g.ttlIndex = appendIndex(g.ttlIndex, IndexEntry{Key: e.Key, ExpiresAt: e.ExpiresAt})

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (g *Group) Get(key []byte) (*CacheEntry, error) {
	var val []byte
	e := CacheEntry{}

	err := g.ttlCache.db.View(func(tx *bolt.Tx) error {

		// Ensure the groups bucket exists
		gbkt := tx.Bucket(g.Key)
		if gbkt == nil {
			return nil
		}
		val = gbkt.Get(key)
		if val == nil {
			return nil
		}

		r := bytes.NewBuffer(val)
		decoder := gob.NewDecoder(r)
		err := decoder.Decode(&e)
		if err != nil {
			return err
		}

		return nil

	})
	if err != nil {
		return nil, err
	}
	if e.Value == nil {
		return nil, nil
	}
	return &e, nil
}

func (g *Group) Delete(key []byte) error {
	g.Lock()
	defer g.Unlock()

	err := g.ttlCache.db.Update(func(tx *bolt.Tx) error {

		// Ensure the groups bucket exists
		gbkt := tx.Bucket(g.Key)
		if gbkt == nil {
			return fmt.Errorf("Bucket does not exist")
		}
		return gbkt.Delete(key)

	})

	if err != nil {
		return err
	}

	ttlIndex := []IndexEntry{}

	for _, e := range g.ttlIndex {
		if !bytes.Equal(e.Key, key) {
			ttlIndex = append(ttlIndex, e)
		}
	}
	g.ttlIndex = ttlIndex
	return nil
}

type IndexEntry struct {
	Key       []byte
	ExpiresAt time.Time
}

type ByIndexEntryExpiry []IndexEntry

func (s ByIndexEntryExpiry) Len() int {
	return len(s)
}
func (s ByIndexEntryExpiry) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByIndexEntryExpiry) Less(i, j int) bool {
	return s[i].ExpiresAt.Before(s[j].ExpiresAt)
}

type CacheEntry struct {
	Key       []byte
	Value     interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
}

// CreateGroupIfNotExists gets a group by key.  If the grouiop does not exist, it will be created.
func (t *TTLCache) CreateGroupIfNotExists(key string) (*Group, error) {

	// Check for an existing group.
	g := t.Groups[key]
	if g != nil {
		return g, nil
	}

	// New Group
	g = &Group{
		Key:      []byte(key),
		ttlIndex: []IndexEntry{},
	}

	g.Lock()
	defer g.Unlock()

	// If the group does not already exist, create it and add it to the database
	err := t.db.Update(func(tx *bolt.Tx) error {
		// get the bucket for the groups
		gps, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
		if err != nil {
			return err
		}
		// Add the group to the bucket
		err = gps.Put(g.Key, g.Key)
		if err != nil {
			return err
		}

		// Now add the group bucket
		_, err = tx.CreateBucketIfNotExists([]byte(key))
		if err != nil {
			return err
		}

		g.ttlCache = t
		t.Groups[key] = g
		return nil
	})
	if err != nil {
		return nil, err
	}

	return g, nil

}

func (t *TTLCache) DeleteGroup(key string) error {
	// Check for an existing group.
	g := t.Groups[key]
	if len(g.Key) == 0 {
		return fmt.Errorf("Group does not exist")
	}

	g.Lock()
	defer g.Unlock()
	// If the group does not already exist, create it and add it to the database
	err := t.db.Update(func(tx *bolt.Tx) error {
		// get the bucket for the groups
		gbkt := tx.Bucket([]byte(groupsBucketName))
		err := gbkt.Delete(g.Key)
		return err
	})
	if err != nil {
		return err
	}

	return nil

}

func (t *TTLCache) Close() {
	t.stop <- true
	t.db.Close()

}

func appendIndex(index []IndexEntry, entry IndexEntry) []IndexEntry {
	newIdx := insertSortedIndexEntry(index, entry, 0, 0)
	return newIdx
}

func insertSortedIndexEntry(index []IndexEntry, entry IndexEntry, start int, end int) []IndexEntry {
	if index == nil {
		return nil
	}

	length := len(index)
	if end == 0 {
		end = length - 1
	}
	mid := start + int(math.Floor(float64(end-start)/2))

	if length == 0 {
		return append(index, entry)
	}

	firstEntry := index[end]
	lastEntry := index[end]
	middleEntry := index[mid]

	if entry.ExpiresAt.Before(firstEntry.ExpiresAt) {
		return append([]IndexEntry{entry}, index...)
	}

	if entry.ExpiresAt.After(lastEntry.ExpiresAt) || entry.ExpiresAt.Equal(lastEntry.ExpiresAt) {
		return append(index, entry)
	}

	if entry.ExpiresAt.Before(middleEntry.ExpiresAt) {
		return insertSortedIndexEntry(index, entry, start, mid-1)
	}

	if entry.ExpiresAt.After(middleEntry.ExpiresAt) || entry.ExpiresAt.Equal(middleEntry.ExpiresAt) {
		return insertSortedIndexEntry(index, entry, mid+1, end)
	}

	return index

}
