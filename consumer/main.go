package consumer

import (
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

type EventSource struct{}

func (s *EventSource) Record() (interface{}, error) {
	pr := &pdk.PilosaRecord{
		Col: 100,
	}
	pr.AddRow("foo", 1)

	return pr, nil
}

/*
type PilosaRecord struct {
    Col  uint64
    Rows []Row
    Vals []Val
}
*/

// Run begins indexing data from Kafka into Pilosa.
func (m *Main) Run() error {
	src := &EventSource{}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := importer.NewIngester(src, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
