package rocks

import (
	"github.com/tecbot/gorocksdb"

	wallet "github.com/marselester/distributed-payment"
)

const (
	// DefaultDB is a default RocksDB database name.
	DefaultDB = "dedup.db"
)

// DedupService represents a RocksDB service to deduplicate requests.
type DedupService struct {
	logger wallet.Logger
	db     *gorocksdb.DB

	copts connOption
}

// connOption holds connection settings.
type connOption struct {
	dbname string
}

// ConfigOption configures the DedupService.
type ConfigOption func(*DedupService)

// WithDB sets the database name.
func WithDB(dbname string) ConfigOption {
	return func(c *DedupService) {
		c.copts.dbname = dbname
	}
}

// WithLogger configures a logger to debug interactions with RocksDB.
func WithLogger(l wallet.Logger) ConfigOption {
	return func(c *DedupService) {
		c.logger = l
	}
}

// NewDedupService returns a DedupService based on RocksDB.
// By default logs are discarded.
func NewDedupService(options ...ConfigOption) *DedupService {
	s := DedupService{
		logger: &wallet.NoopLogger{},
		copts: connOption{
			dbname: DefaultDB,
		},
	}

	for _, opt := range options {
		opt(&s)
	}
	return &s
}

// Open opens a connection to RocksDB.
func (s *DedupService) Open() error {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	var err error
	s.db, err = gorocksdb.OpenDb(opts, s.copts.dbname)
	return err
}

// Close closes the database.
func (s *DedupService) Close() {
	s.db.Close()
}

// HasSeen checks whether the request ID is a duplicate.
func (s *DedupService) HasSeen(requestID string) (bool, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	value, err := s.db.Get(ro, []byte(requestID))
	if err != nil {
		return false, err
	}
	defer value.Free()

	if len(value.Data()) == 0 {
		s.logger.Log("level", "debug", "msg", "rocks did not find request", "request", requestID)
		return false, nil
	}

	s.logger.Log("level", "debug", "msg", "rocks found request", "request", requestID)
	return true, nil
}

// Save persists the requestID in the database to discard request duplicates.
func (s *DedupService) Save(requestID string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	err := s.db.Put(wo, []byte(requestID), []byte{1})
	if err != nil {
		s.logger.Log("level", "debug", "msg", "rocks did not save request", "request", requestID, "err", err)
		return err
	}

	s.logger.Log("level", "debug", "msg", "rocks saved request", "request", requestID)
	return nil
}
