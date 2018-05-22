package consumer

import (
	"fmt"
	"math/rand"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
	"github.com/travisturner/importer"
)

// Main holds the options for running Pilosa ingestion from Kafka.
type Main struct {
	PilosaHosts []string `help:"Comma separated list of Pilosa hosts and ports."`
	Index       string   `help:"Pilosa index."`
	BatchSize   uint     `help:"Batch size for Pilosa imports (latency/throughput tradeoff)."`
}

// NewMain returns a new Main.
func NewMain() *Main {
	return &Main{
		PilosaHosts: []string{"localhost:10101"},
		Index:       "iot",
		BatchSize:   1000,
	}
}

type EventSource struct {
	Frames []string

	count int
}

func (s *EventSource) Record() (interface{}, error) {
	pr := &pdk.PilosaRecord{
		//Col: uint64(rand.Intn(100000000)),
		Col: uint64(s.count),
	}
	for _, f := range s.Frames {
		pr.AddRow(f, uint64(rand.Intn(100)))
	}
	pr.AddVal("bb", "fld", int64(rand.Intn(100000)))
	s.count++

	if s.count%100000 == 0 {
		fmt.Println("count:", s.count)
	}

	return pr, nil
}

// Run begins indexing data into Pilosa.
func (m *Main) Run() error {
	src := &EventSource{
		Frames: []string{"fa", "fb", "fc", "fd", "fe", "ff", "fg", "fh", "fi", "fj"},
	}

	fields := []pdk.FieldSpec{
		pdk.FieldSpec{
			Name: "fld",
			Min:  0,
			Max:  100000,
		},
	}

	frames := []pdk.FrameSpec{
		pdk.FrameSpec{
			Name:           "bb",
			CacheSize:      0,
			InverseEnabled: false,
			Fields:         fields,
		},
	}

	//indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, frames, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := importer.NewIngester(src, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
