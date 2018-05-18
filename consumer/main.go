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
		//pr.AddRow(f, 1)
	}
	s.count++

	if s.count%100000 == 0 {
		fmt.Println("count:", s.count)
	}

	return pr, nil
}

/*
type PilosaRecord struct {
    Col  uint64
    Rows []Row
    Vals []Val
}

// AddVal adds a new value to be range encoded into the given field to the
// PilosaRecord.
func (pr *PilosaRecord) AddVal(frame, field string, value int64) {
    pr.Vals = append(pr.Vals, Val{Frame: frame, Field: field, Value: value})
}

// AddRow adds a new bit to be set to the PilosaRecord.
func (pr *PilosaRecord) AddRow(frame string, id uint64) {
    pr.Rows = append(pr.Rows, Row{Frame: frame, ID: id})
}

// AddRowTime adds a new bit to be set with a timestamp to the PilosaRecord.
func (pr *PilosaRecord) AddRowTime(frame string, id uint64, ts time.Time) {
    pr.Rows = append(pr.Rows, Row{Frame: frame, ID: id, Time: ts})
}

// Row represents a bit to set in Pilosa sans column id (which is held by the
// PilosaRecord containg the Row).
type Row struct {
    Frame string
    ID    uint64

    // Time is the timestamp for the bit in Pilosa which is the intersection of
    // this row and the Column in the PilosaRecord which holds this row.
    Time time.Time
}

// Val represents a BSI value to set in a Pilosa field sans column id (which is
// held by the PilosaRecord containing the Val).
type Val struct {
    Frame string
    Field string
    Value int64
}

*/

// Run begins indexing data from Kafka into Pilosa.
func (m *Main) Run() error {
	src := &EventSource{
		Frames: []string{"fa", "fb", "fc", "fd", "fe", "ff", "fg", "fh", "fi", "fj"},
		//Frames: []string{"fa"},
	}

	indexer, err := pdk.SetupPilosa(m.PilosaHosts, m.Index, []pdk.FrameSpec{}, m.BatchSize)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	ingester := importer.NewIngester(src, indexer)
	return errors.Wrap(ingester.Run(), "running ingester")
}
