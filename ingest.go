package importer

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/pilosa/pdk"
	"github.com/pilosa/pdk/termstat"
)

// Ingester combines a Source, Parser, Mapper, and Indexer, and uses them to
// ingest data into Pilosa. This could be a streaming situation where the Source
// never ends, and calling it just waits for more data to be available, or a
// batch situation where the Source eventually returns io.EOF (or some other
// error), and the Ingester completes (after the other components are done).
type Ingester struct {
	ParseConcurrency int

	src     pdk.Source
	indexer pdk.Indexer

	colCount uint

	Stats pdk.Statter
	Log   pdk.Logger
}

// NewIngester gets a new Ingester.
func NewIngester(source pdk.Source, indexer pdk.Indexer, colCount uint) *Ingester {
	return &Ingester{
		ParseConcurrency: 1,

		src:     source,
		indexer: indexer,

		colCount: colCount,

		// Reasonable defaults for crosscutting dependencies.
		Stats: termstat.NewCollector(os.Stdout),
		Log:   pdk.StdLogger{log.New(os.Stderr, "Ingest", log.LstdFlags)},
	}
}

// Run runs the ingest.
func (n *Ingester) Run() error {
	pwg := sync.WaitGroup{}
	for i := 0; i < n.ParseConcurrency; i++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			var recordErr error
			ii := 0
			for {
				if ii >= int(n.colCount) {
					return
				}
				ii++
				// Source
				var rec interface{}
				rec, recordErr = n.src.Record()
				if recordErr != nil {
					break
				}
				n.Stats.Count("ingest.Record", 1, 1)

				pr := rec.(*pdk.PilosaRecord)

				// Index
				for _, row := range pr.Rows {
					n.indexer.AddBit(row.Frame, pr.Col, row.ID)
					n.Stats.Count("ingest.AddBit", 1, 1)
				}
				for _, val := range pr.Vals {
					n.indexer.AddValue(val.Frame, val.Field, pr.Col, val.Value)
					n.Stats.Count("ingest.AddValue", 1, 1)
				}
			}
			if recordErr != io.EOF && recordErr != nil {
				n.Log.Printf("error in ingest run loop: %v", recordErr)
			}
		}()
	}
	pwg.Wait()
	return n.indexer.Close()
}
