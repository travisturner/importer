package consumer

import (
	"fmt"
	"math/rand"

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
			pr.AddVal(f.Name, "fld", int64(rand.Intn(1000000)))
		}
	}
	s.count++

	if s.count%100000 == 0 {
		fmt.Println("count:", s.count)
	}

	return pr, nil
}

// Run begins indexing data into Pilosa.
func (m *Main) Run() error {
	frames := []pdk.FrameSpec{}

	// Bit Frames
	for i := 0; i < int(m.FrameCount); i++ {
		frames = append(frames, pdk.FrameSpec{
			Name:           fmt.Sprintf("f%d", i),
			CacheSize:      0,
			InverseEnabled: false,
		})
	}

	// BSI Frames
	for i := 0; i < int(m.ValCount); i++ {
		fields := []pdk.FieldSpec{
			pdk.FieldSpec{
				Name: "fld",
				Min:  0,
				Max:  1000000,
			},
		}
		frames = append(frames, pdk.FrameSpec{
			Name:           fmt.Sprintf("v%d", i),
			CacheSize:      0,
			InverseEnabled: false,
			Fields:         fields,
		})
	}

	src := &EventSource{
		Frames: frames,
	}

	// Indexer
	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, frames,
		gopilosa.OptImportStrategy(gopilosa.BatchImport),
		gopilosa.OptImportBatchSize(int(m.BatchSize)))

	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := importer.NewIngester(src, indexer, m.ColCount)
	return errors.Wrap(ingester.Run(), "running ingester")
}
