package command

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/delaneyj/witchbolt"
)

var benchBucketName = []byte("bench")

type benchOptions struct {
	profileMode     string
	writeMode       string
	readMode        string
	iterations      int64
	batchSize       int64
	keySize         int
	valueSize       int
	cpuProfile      string
	memProfile      string
	blockProfile    string
	fillPercent     float64
	noSync          bool
	work            bool
	path            string
	goBenchOutput   bool
	pageSize        int
	initialMmapSize int
	deleteFraction  float64 // Fraction of keys of last tx to delete during writes. works only with "seq-del" write mode.
	explicitPath    bool
}

type benchIO struct {
	stdout io.Writer
	stderr io.Writer
}

type BenchCmd struct {
	ProfileMode     string  `name:"profile-mode" default:"rw" help:"Profiling mode: rw (writes then reads), r (reads only), w (writes only)."`
	WriteMode       string  `name:"write-mode" default:"seq" enum:"seq,rnd,seq-nest,rnd-nest,seq-del" help:"Pattern used for write operations."`
	ReadMode        string  `name:"read-mode" default:"seq" enum:"seq,rnd" help:"Pattern used for read operations."`
	Count           int64   `name:"count" default:"1000" help:"Number of benchmark iterations."`
	BatchSize       int64   `name:"batch-size" default:"0" help:"Batch size per transaction. Defaults to count when zero."`
	KeySize         int     `name:"key-size" default:"8" help:"Size of keys in bytes."`
	ValueSize       int     `name:"value-size" default:"32" help:"Size of values in bytes."`
	CPUProfile      string  `name:"cpuprofile" help:"Write CPU profile to the specified file."`
	MemProfile      string  `name:"memprofile" help:"Write heap profile to the specified file."`
	BlockProfile    string  `name:"blockprofile" help:"Write block profile to the specified file."`
	FillPercent     float64 `name:"fill-percent" default:"0.5" help:"Fill percentage used for buckets."`
	NoSync          bool    `name:"no-sync" help:"Disable fsync for the destination database."`
	Work            bool    `name:"work" help:"Keep the generated database file (implies printing its path)."`
	Path            string  `name:"path" help:"Existing database file to benchmark; if omitted, a temporary file is created." type:"path"`
	GoBenchOutput   bool    `name:"gobench-output" help:"Emit results in go test benchmark format."`
	PageSize        int     `name:"page-size" default:"4096" help:"Database page size in bytes."`
	InitialMmapSize int     `name:"initial-mmap-size" default:"0" help:"Initial mmap size in bytes for database file."`
}

func (c *BenchCmd) Run() error {
	options := benchOptions{
		profileMode:     c.ProfileMode,
		writeMode:       c.WriteMode,
		readMode:        c.ReadMode,
		iterations:      c.Count,
		batchSize:       c.BatchSize,
		keySize:         c.KeySize,
		valueSize:       c.ValueSize,
		cpuProfile:      c.CPUProfile,
		memProfile:      c.MemProfile,
		blockProfile:    c.BlockProfile,
		fillPercent:     c.FillPercent,
		noSync:          c.NoSync,
		work:            c.Work,
		path:            c.Path,
		goBenchOutput:   c.GoBenchOutput,
		pageSize:        c.PageSize,
		initialMmapSize: c.InitialMmapSize,
		explicitPath:    c.Path != "",
	}

	if err := options.Validate(); err != nil {
		return err
	}
	if err := options.SetOptionValues(); err != nil {
		return err
	}

	io := benchIO{stdout: os.Stdout, stderr: os.Stderr}
	return benchFunc(io, &options)
}

// Returns an error if `bench` options are not valid.
func (o *benchOptions) Validate() error {
	// Require that batch size can be evenly divided by the iteration count if set.
	if o.batchSize > 0 && o.iterations%o.batchSize != 0 {
		return ErrBatchNonDivisibleBatchSize
	}

	switch o.writeMode {
	case "seq", "rnd", "seq-nest", "rnd-nest":
	default:
		return ErrBatchInvalidWriteMode
	}

	// Generate temp path if one is not passed in.
	if o.path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		o.path = f.Name()
	}

	return nil
}

// Sets the `bench` option values that are dependent on other options.
func (o *benchOptions) SetOptionValues() error {
	// Generate temp path if one is not passed in.
	if o.path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return fmt.Errorf("error creating temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		o.path = f.Name()
	}

	// Set batch size to iteration size if not set.
	if o.batchSize == 0 {
		o.batchSize = o.iterations
	}

	return nil
}

func benchFunc(io benchIO, options *benchOptions) error {
	if options.work {
		fmt.Fprintf(io.stderr, "work: %s\n", options.path)
	}

	if !options.work && !options.explicitPath {
		defer os.Remove(options.path)
	}

	// Create database.
	dbOptions := *witchbolt.DefaultOptions
	dbOptions.PageSize = options.pageSize
	dbOptions.InitialMmapSize = options.initialMmapSize
	db, err := witchbolt.Open(options.path, 0600, &dbOptions)
	if err != nil {
		return err
	}
	db.NoSync = options.noSync
	defer db.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var writeResults benchResults

	fmt.Fprintf(io.stderr, "starting write benchmark.\n")
	keys, err := runWrites(io, db, options, &writeResults, r)
	if err != nil {
		return fmt.Errorf("write: %v", err)
	}

	if keys != nil {
		r.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
	}

	var readResults benchResults
	fmt.Fprintf(io.stderr, "starting read benchmark.\n")
	// Read from the database.
	if err := runReads(io, db, options, &readResults, keys); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	if options.goBenchOutput {
		// below replicates the output of testing.B benchmarks, e.g. for external tooling
		benchWriteName := "BenchmarkWrite"
		benchReadName := "BenchmarkRead"
		maxLen := max(len(benchReadName), len(benchWriteName))
		printGoBenchResult(io.stdout, writeResults, maxLen, benchWriteName)
		printGoBenchResult(io.stdout, readResults, maxLen, benchReadName)
	} else {
		fmt.Fprintf(io.stdout, "# Write\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", writeResults.getCompletedOps(), writeResults.getDuration(), writeResults.opDuration(), writeResults.opsPerSecond())
		fmt.Fprintf(io.stdout, "# Read\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", readResults.getCompletedOps(), readResults.getDuration(), readResults.opDuration(), readResults.opsPerSecond())
	}
	fmt.Fprintln(io.stdout, "")

	return nil
}

func runWrites(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	// Start profiling for writes.
	if options.profileMode == "rw" || options.profileMode == "w" {
		if err := startProfiling(options); err != nil {
			return nil, err
		}
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, io.stderr)
	defer close(finishChan)

	t := time.Now()

	var keys []nestedKey
	var err error
	switch options.writeMode {
	case "seq":
		keys, err = runWritesSequential(io, db, options, results)
	case "rnd":
		keys, err = runWritesRandom(io, db, options, results, r)
	case "seq-nest":
		keys, err = runWritesSequentialNested(io, db, options, results)
	case "rnd-nest":
		keys, err = runWritesRandomNested(io, db, options, results, r)
	case "seq-del":
		options.deleteFraction = 0.1
		keys, err = runWritesSequentialAndDelete(io, db, options, results)
	default:
		return nil, fmt.Errorf("invalid write mode: %s", options.writeMode)
	}

	// Save time to write.
	results.setDuration(time.Since(t))

	// Stop profiling for writes only.
	if options.profileMode == "w" {
		if err := stopProfiling(); err != nil {
			return keys, err
		}
	}

	return keys, err
}

func runWritesSequential(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesWithSource(io, db, options, results, func() uint32 { i++; return i })
}

func runWritesSequentialAndDelete(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesDeletesWithSource(io, db, options, results, func() uint32 { i++; return i })
}

func runWritesRandom(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	return runWritesWithSource(io, db, options, results, func() uint32 { return r.Uint32() })
}

func runWritesSequentialNested(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesNestedWithSource(io, db, options, results, func() uint32 { i++; return i })
}

func runWritesRandomNested(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	return runWritesNestedWithSource(io, db, options, results, func() uint32 { return r.Uint32() })
}

func runWritesWithSource(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.readMode == "rnd" {
		keys = make([]nestedKey, 0, options.iterations)
	}

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *witchbolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.fillPercent

			fmt.Fprintf(io.stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {
				key := make([]byte, options.keySize)
				value := make([]byte, options.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{nil, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(io.stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runWritesDeletesWithSource(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	deleteSize := int64(math.Ceil(float64(options.batchSize) * options.deleteFraction))
	var InsertedKeys [][]byte

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *witchbolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.fillPercent

			fmt.Fprintf(io.stderr, "Starting delete iteration %d, deleteSize: %d\n", i, deleteSize)
			for i := int64(0); i < deleteSize && i < int64(len(InsertedKeys)); i++ {
				if err := b.Delete(InsertedKeys[i]); err != nil {
					return err
				}
			}
			InsertedKeys = InsertedKeys[:0]
			fmt.Fprintf(io.stderr, "Finished delete iteration %d\n", i)

			fmt.Fprintf(io.stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {

				key := make([]byte, options.keySize)
				value := make([]byte, options.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())
				InsertedKeys = append(InsertedKeys, key)

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{nil, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(io.stderr, "Finished write iteration %d\n", i)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runWritesNestedWithSource(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.readMode == "rnd" {
		keys = make([]nestedKey, 0, options.iterations)
	}

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *witchbolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = options.fillPercent

			// Create bucket key.
			name := make([]byte, options.keySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = options.fillPercent

			fmt.Fprintf(io.stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {
				var key = make([]byte, options.keySize)
				var value = make([]byte, options.valueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{name, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(io.stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runReads(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults, keys []nestedKey) error {
	// Start profiling for reads.
	if options.profileMode == "r" {
		if err := startProfiling(options); err != nil {
			return err
		}
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, io.stderr)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch options.readMode {
	case "seq":
		switch options.writeMode {
		case "seq-nest", "rnd-nest":
			err = runReadsSequentialNested(io, db, options, results)
		default:
			err = runReadsSequential(io, db, options, results)
		}
	case "rnd":
		switch options.writeMode {
		case "seq-nest", "rnd-nest":
			err = runReadsRandomNested(io, db, options, keys, results)
		default:
			err = runReadsRandom(io, db, options, keys, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.readMode)
	}

	// Save read time.
	results.setDuration(time.Since(t))

	// Stop profiling for reads.
	if options.profileMode == "rw" || options.profileMode == "r" {
		if stopErr := stopProfiling(); stopErr != nil {
			return stopErr
		}
	}

	return err
}

type nestedKey struct{ bucket, key []byte }

func runReadsSequential(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults) error {
	return db.View(func(tx *witchbolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

				c := tx.Bucket(benchBucketName).Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					numReads++
					if v == nil {
						return ErrInvalidValue
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.writeMode == "seq" && numReads != options.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsRandom(io benchIO, db *witchbolt.DB, options *benchOptions, keys []nestedKey, results *benchResults) error {
	return db.View(func(tx *witchbolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

				b := tx.Bucket(benchBucketName)
				for _, key := range keys {
					v := b.Get(key.key)
					numReads++
					if v == nil {
						return ErrInvalidValue
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.writeMode == "seq" && numReads != options.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsSequentialNested(io benchIO, db *witchbolt.DB, options *benchOptions, results *benchResults) error {
	return db.View(func(tx *witchbolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				defer func() { results.addCompletedOps(numReads) }()
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						numReads++
						if v == nil {
							return ErrInvalidValue
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if options.writeMode == "seq-nest" && numReads != options.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsRandomNested(io benchIO, db *witchbolt.DB, options *benchOptions, nestedKeys []nestedKey, results *benchResults) error {
	return db.View(func(tx *witchbolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

				var top = tx.Bucket(benchBucketName)
				for _, nestedKey := range nestedKeys {
					if b := top.Bucket(nestedKey.bucket); b != nil {
						v := b.Get(nestedKey.key)
						numReads++
						if v == nil {
							return ErrInvalidValue
						}
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.writeMode == "seq-nest" && numReads != options.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func checkProgress(results *benchResults, finishChan chan interface{}, stderr io.Writer) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.getCompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
}

var cpuprofile, memprofile, blockprofile *os.File

func startProfiling(options *benchOptions) error {
	// Start CPU profiling.
	if options.cpuProfile != "" {
		file, err := os.Create(options.cpuProfile)
		if err != nil {
			return fmt.Errorf("bench: could not create cpu profile %q: %w", options.cpuProfile, err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			file.Close()
			return fmt.Errorf("bench: could not start cpu profile %q: %w", options.cpuProfile, err)
		}
		cpuprofile = file
	}

	// Start memory profiling.
	if options.memProfile != "" {
		file, err := os.Create(options.memProfile)
		if err != nil {
			return fmt.Errorf("bench: could not create memory profile %q: %w", options.memProfile, err)
		}
		memprofile = file
		runtime.MemProfileRate = 4096
	}

	// Start block profiling.
	if options.blockProfile != "" {
		file, err := os.Create(options.blockProfile)
		if err != nil {
			return fmt.Errorf("bench: could not create block profile %q: %w", options.blockProfile, err)
		}
		blockprofile = file
		runtime.SetBlockProfileRate(1)
	}

	return nil
}

func stopProfiling() error {
	var errs []error

	if cpuprofile != nil {
		pprof.StopCPUProfile()
		if err := cpuprofile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("bench: closing cpu profile: %w", err))
		}
		cpuprofile = nil
	}

	if memprofile != nil {
		if err := pprof.Lookup("heap").WriteTo(memprofile, 0); err != nil {
			errs = append(errs, fmt.Errorf("bench: could not write mem profile: %w", err))
		}
		if err := memprofile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("bench: closing mem profile: %w", err))
		}
		memprofile = nil
	}

	if blockprofile != nil {
		if err := pprof.Lookup("block").WriteTo(blockprofile, 0); err != nil {
			errs = append(errs, fmt.Errorf("bench: could not write block profile: %w", err))
		}
		if err := blockprofile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("bench: closing block profile: %w", err))
		}
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// benchResults represents the performance results of the benchmark and is thread-safe.
type benchResults struct {
	completedOps int64
	duration     int64
}

func (r *benchResults) addCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *benchResults) getCompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *benchResults) setDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *benchResults) getDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// opDuration returns the duration for a single read/write operation.
func (r *benchResults) opDuration() time.Duration {
	if r.getCompletedOps() == 0 {
		return 0
	}
	return r.getDuration() / time.Duration(r.getCompletedOps())
}

// opsPerSecond returns average number of read/write operations that can be performed per second.
func (r *benchResults) opsPerSecond() int {
	var op = r.opDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

func printGoBenchResult(w io.Writer, r benchResults, maxLen int, benchName string) {
	gobenchResult := testing.BenchmarkResult{}
	gobenchResult.T = r.getDuration()
	gobenchResult.N = int(r.getCompletedOps())
	fmt.Fprintf(w, "%-*s\t%s\n", maxLen, benchName, gobenchResult.String())
}
