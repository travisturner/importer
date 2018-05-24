package consumer

import (
	"fmt"
	"math"
	"math/rand"
	"sync"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
	"github.com/travisturner/importer"
)

// Main holds the options for running Pilosa ingestion from Kafka.
type Main struct {
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
	FrameCount  uint     `help:"Number of Frames to write single bits to."`
	ValCount    uint     `help:"Number of Frames to write values to."`
	ColCount    uint     `help:"Number of Columns to create."`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		PilosaHosts: []string{"localhost:10101"},
		Index:       "iot",
		BatchSize:   1000,
		FrameCount:  1,
		ValCount:    1,
		ColCount:    1000,
	}
}

type EventSource struct {
	Frames []pdk.FrameSpec

	count int
}

func (s *EventSource) Record() (interface{}, error) {
	pr := &pdk.PilosaRecord{
		Col: uint64(s.count),
	}
	for _, f := range s.Frames {
		if len(f.Fields) == 0 {
			pr.AddRow(f.Name, uint64(rand.Intn(100)))
		} else {
			pr.AddVal(f.Name, f.Name, int64(rand.Intn(1000000))) // NOTE: this assumes a single field with Name = frame.Name
		}
	}
	s.count++

	return pr, nil
}

// Run begins indexing data into Pilosa.
func (m *Main) Run() error {
	frames := []pdk.FrameSpec{}

	statusChans := []*importStatusChannel{}

	// Bit Frames
	for i := 0; i < int(m.FrameCount); i++ {
		frameName := fmt.Sprintf("f%d", i)
		ch := make(chan gopilosa.ImportStatusUpdate, 1000)
		statusChans = append(statusChans, &importStatusChannel{
			frame: frameName,
			ch:    ch,
		})
		frames = append(frames, pdk.NewRankedFrameSpec(frameName, 0,
			gopilosa.OptImportStatusChannel(ch),
			gopilosa.OptImportStrategy(gopilosa.BatchImport),
			gopilosa.OptImportBatchSize(int(m.BatchSize)),
		))
	}

	// BSI Frames
	for i := 0; i < int(m.ValCount); i++ {
		frameName := fmt.Sprintf("v%d", i)
		ch := make(chan gopilosa.ImportStatusUpdate, 1000)
		statusChans = append(statusChans, &importStatusChannel{
			frame: frameName,
			ch:    ch,
		})
		frames = append(frames, pdk.NewFieldFrameSpec(frameName, 0, math.MaxUint32,
			gopilosa.OptImportStatusChannel(ch),
			gopilosa.OptImportStrategy(gopilosa.BatchImport),
			gopilosa.OptImportBatchSize(int(m.BatchSize)),
		))
	}

	fmt.Println("frames: %v\n", frames)

	src := &EventSource{
		Frames: frames,
	}

	// Indexer
	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, frames)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := importer.NewIngester(src, indexer, m.ColCount)

	// take status off the channel and add them to indexer.Stats
	go func() {
		for status := range merge(statusChans...) {
			// act on the status update
			fmt.Printf("STATUS - frame: %s, thread: %d, slice: %d, count: %d, time: %v\n", status.frame, status.ThreadID, status.Slice, status.ImportedCount, status.Time)
		}
	}()

	return errors.Wrap(ingester.Run(), "running ingester")
}

type importStatus struct {
	gopilosa.ImportStatusUpdate
	frame string
}

type importStatusChannel struct {
	frame string
	ch    <-chan gopilosa.ImportStatusUpdate
}

func merge(cs ...*importStatusChannel) <-chan importStatus {
	var wg sync.WaitGroup
	out := make(chan importStatus)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c *importStatusChannel) {
		for n := range c.ch {
			out <- importStatus{
				ImportStatusUpdate: n,
				frame:              c.frame,
			}
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
