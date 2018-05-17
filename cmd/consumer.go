package cmd

import (
	"io"
	"log"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/spf13/cobra"
	"github.com/travisturner/importer/consumer"
)

// ConsumerMain is wrapped by NewConsumerCommand and only exported for testing
// purposes.
var ConsumerMain *consumer.Main

// NewConsumerCommand returns a new cobra command wrapping ConsumerMain.
func NewConsumerCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	var err error
	ConsumerMain = consumer.NewMain()
	consumerCommand := &cobra.Command{
		Use:   "consumer",
		Short: "consumer - read messages from kafka source and put into Pilosa",
		Long:  `TODO`,
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err = ConsumerMain.Run()
			if err != nil {
				return err
			}
			log.Println("Done: ", time.Since(start))
			select {}
		},
	}
	flags := consumerCommand.Flags()
	err = commandeer.Flags(flags, ConsumerMain)
	if err != nil {
		panic(err)
	}
	return consumerCommand
}

func init() {
	subcommandFns["consumer"] = NewConsumerCommand
}
