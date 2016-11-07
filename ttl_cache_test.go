package ttlcache

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TTLCache", func() {
	var (
		ttlCache    *TTLCache
		storagePath string
	)

	BeforeEach(func() {
		f, err := ioutil.TempFile("", "example")
		if err != nil {
			Fail(err.Error())
		}

		storagePath = f.Name()
		// cose the temp file, but leave it there
		f.Close()

		ttlCache, err = OpenCache(storagePath, time.Duration(0))
		if err != nil {
			Fail(err.Error())
		}

	})

	AfterEach(func() {
		ttlCache.Close()
		os.RemoveAll(storagePath)
	})

	Context("Working with cache", func() {
		It("should create the database file when the cache is opened", func() {
			stat, err := os.Stat(storagePath)

			Expect(err).To(BeNil())
			Expect(stat.Size()).To(BeNumerically(">", 0))
		})
	})

	Context("Working with groups", func() {
		It("should add the group to the database when a group is initially added", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			// check the contents of the database

			ttlCache.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(groupsBucketName))
				v := b.Get([]byte(defaultGroupName))

				Expect(v).NotTo(BeNil())
				Expect(v).To(BeEquivalentTo(defaultGroupName))
				return nil
			})
		})

		It("should add the group bucket to the database when a group is initially added", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			// check the contents of the database
			ttlCache.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(defaultGroupName))
				Expect(b).NotTo(BeNil())
				return nil
			})
		})

		It("should remove the group from database when a group is removed", func() {
			// check the contents of the database
			ttlCache.CreateGroupIfNotExists("shorty")

			err := ttlCache.DeleteGroup("shorty")
			Expect(err).To(BeNil())

			ttlCache.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(groupsBucketName))
				v := b.Get([]byte("shorty"))
				Expect(len(v)).To(Equal(0))

				return nil
			})
		})

	})

	Context("Working with keys", func() {
		It("Saves the keys for a group when provided", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			// check the contents of the database

			ttlCache.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(groupsBucketName))
				v := b.Get([]byte(defaultGroupName))

				Expect(v).NotTo(BeNil())
				Expect(v).To(BeEquivalentTo(defaultGroupName))

				// now get the ensure the key is in the group bucket
				gbkt, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
				if err != nil {
					return err
				}

				val := gbkt.Get([]byte("key"))
				Expect(val).To(BeEquivalentTo("string"))
				return nil
			})
		})

		It("Adds a new entry to the index as keys are added", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			// check the contents of the database
			Expect(len(g.ttlIndex)).To(BeNumerically(">", 0))

		})

		It("Retieves saved keys", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			val, err := g.Get([]byte("key"))
			Expect(err).To(BeNil())
			Expect(val.Value).To(BeEquivalentTo("string"))

		})

		It("Returns nil when retieving a key that does not exist", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			val, err := g.Get([]byte("notkey"))
			Expect(err).To(BeNil())
			Expect(val).To(BeNil())

		})

		It("Removes saved keys after ttl expires", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			time.Sleep(20 * time.Millisecond)

			val, err := g.Get([]byte("key"))
			Expect(err).To(BeNil())
			Expect(val).To(BeNil())

		})

		It("Removes the index entry when the entry is removed", func() {
			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))

			g.Put([]byte("key"), []byte("string"), time.Millisecond)

			// check the contents of the database
			Expect(len(g.ttlIndex)).To(BeNumerically(">", 0))
			g.Delete([]byte("key"))

			g.Lock()

			Expect(len(g.ttlIndex)).To(Equal(0))
			g.Unlock()

		})

	})
	Context("Working concurrently with keys", func() {
		It("Saves the keys for a group when provided", func() {
			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// introduce a random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					g.Put(key, tval, time.Hour)

					// check the contents of the database

					ttlCache.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte(groupsBucketName))
						v := b.Get([]byte(defaultGroupName))

						Expect(v).NotTo(BeNil())
						Expect(v).To(BeEquivalentTo(defaultGroupName))

						// now get the ensure the key is in the group bucket
						gbkt, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
						if err != nil {
							return err
						}

						val := gbkt.Get(key)
						Expect(val).To(BeEquivalentTo(tval))
						return nil
					})

				}(idx)
			}

			wg.Wait()
			g.Lock()
			defer g.Unlock()

			Expect(len(g.ttlIndex)).To(Equal(1000))
		})

		It("Retieves saved keys", func() {
			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// introduce a random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					g.Put(key, tval, time.Hour)

					// check the contents of the database

					ttlCache.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte(groupsBucketName))
						v := b.Get([]byte(defaultGroupName))

						Expect(v).NotTo(BeNil())
						Expect(v).To(BeEquivalentTo(defaultGroupName))

						// now get the ensure the key is in the group bucket
						gbkt, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
						if err != nil {
							return err
						}

						val := gbkt.Get(key)
						Expect(val).To(BeEquivalentTo(tval))
						return nil
					})

				}(idx)
			}

			wg.Wait()

			wg = sync.WaitGroup{}

			//Now wait until all of the keys have expired

			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					v, err := g.Get(key)
					Expect(err).To(BeNil())
					Expect(v).NotTo(BeNil())
					Expect(v.Value).To(Equal(tval))

				}(idx)
			}

			wg.Wait()

		})

		It("Removes saved keys after ttl expires", func() {
			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					// introduce a random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					g.Put(key, tval, time.Millisecond)

					// check the contents of the database

					ttlCache.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte(groupsBucketName))
						v := b.Get([]byte(defaultGroupName))

						Expect(v).NotTo(BeNil())
						Expect(v).To(BeEquivalentTo(defaultGroupName))

						// now get the ensure the key is in the group bucket
						gbkt, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
						if err != nil {
							return err
						}

						val := gbkt.Get(key)
						Expect(val).To(BeEquivalentTo(tval))
						return nil
					})

					wg.Done()

				}(idx)
			}

			wg.Wait()

			//Now wait until all of the keys have expired
			time.Sleep(time.Second)

			wg = sync.WaitGroup{}

			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					key := []byte(fmt.Sprintf("key%d", i))

					_, err := g.Get(key)
					Expect(err).To(BeNil())

				}(idx)
			}

			wg.Wait()

		})

		It("Removes the index entry when the entry is removed", func() {
			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					// introduce a random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					g.Put(key, tval, time.Millisecond)

					// check the contents of the database

					ttlCache.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte(groupsBucketName))
						v := b.Get([]byte(defaultGroupName))

						Expect(v).NotTo(BeNil())
						Expect(v).To(BeEquivalentTo(defaultGroupName))

						// now get the ensure the key is in the group bucket
						gbkt, err := tx.CreateBucketIfNotExists([]byte(groupsBucketName))
						if err != nil {
							return err
						}

						val := gbkt.Get(key)
						Expect(val).To(BeEquivalentTo(tval))
						return nil
					})

					wg.Done()

				}(idx)
			}

			wg.Wait()

			//Now wait until all of the keys have expired
			time.Sleep(time.Second)

			Expect(len(g.ttlIndex)).To(Equal(0))

		})

		XIt("Persists cache data and rebuild index properly when opened", func() {
			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// introduce a random delay
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					g.Put(key, tval, time.Hour)

				}(idx)
			}

			wg.Wait()

			// now close the ttlCache and reopen
			ttlCache.Close()

			ttlCache = nil

			ttlCache, err = OpenCache(storagePath, time.Duration(0))
			Expect(err).To(BeNil())
			ng, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())

			wg = sync.WaitGroup{}

			// Now see if the leys survived

			for idx := 0; idx < 1000; idx++ {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()
					key := []byte(fmt.Sprintf("key%d", i))
					tval := []byte(fmt.Sprintf("val%d", i))

					v, err := ng.Get(key)
					Expect(err).To(BeNil())
					Expect(v).NotTo(BeNil())
					Expect(v.Value).To(Equal(tval))

				}(idx)
			}

			wg.Wait()

			// now make sure this index got created
			Expect(len(ng.ttlIndex)).To(Equal(1000))

		})
	})
	Context("Handling many keys", func() {
		XIt("should still perform well even with 10,000 keys", func() {

			ttlCache.Close()
			ttlCache, err := OpenCache("./cache.db", time.Second)

			wg := sync.WaitGroup{}

			g, err := ttlCache.CreateGroupIfNotExists(defaultGroupName)
			Expect(err).To(BeNil())
			Expect(g.Key).To(BeEquivalentTo(defaultGroupName))
			for idx := 0; idx < 10000; idx++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := []byte(fmt.Sprintf("key%d", i))
					tval := map[string]string{}
					tval["key"] = strconv.Itoa(i)
					tval["time"] = time.Now().String()

					g.Put(key, tval, 3*time.Minute)

					// check the contents of the database

				}(idx)
			}

			wg.Wait()
			g.Lock()
			defer g.Unlock()

			Expect(len(g.ttlIndex)).To(Equal(10000))
		})

	})
})
