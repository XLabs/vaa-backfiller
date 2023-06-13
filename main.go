package main

import (
	"github.com/XLabs/vaa-backfiller/backfiller"
	"github.com/XLabs/vaa-backfiller/txhash"
	"github.com/spf13/cobra"
)

func main() {
	execute()
}

func execute() error {
	root := &cobra.Command{
		Use: "backfiller",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				backfiller.Run()
			}
		},
	}

	addVaaBackfillerCommand(root)
	addTxHashFixFormatCommand(root)

	return root.Execute()
}

func addVaaBackfillerCommand(root *cobra.Command) {
	var mongoUri, mongoDb, filename string
	var workerCount int

	vaaBackfillerCommand := &cobra.Command{
		Use:   "vaa",
		Short: "Run vaa backfiller",
		Run: func(_ *cobra.Command, _ []string) {
			backfiller.Run(
				&backfiller.BackfillerConfiguration{
					MongoURI:      mongoUri,
					MongoDatabase: mongoDb,
					Filename:      filename,
					WorkerCount:   workerCount,
				},
			)
		},
	}
	vaaBackfillerCommand.Flags().StringVar(&mongoUri, "mongo-uri", "", "Mongo connection")
	vaaBackfillerCommand.Flags().StringVar(&mongoDb, "mongo-database", "", "Mongo database")
	vaaBackfillerCommand.Flags().StringVar(&filename, "filename", "", "vaa backfiller filename")
	vaaBackfillerCommand.Flags().IntVar(&workerCount, "worker-count", 100, "backfiller worker count")
	root.AddCommand(vaaBackfillerCommand)

}

func addTxHashFixFormatCommand(root *cobra.Command) {
	txHashFixFormatCommand := &cobra.Command{
		Use:   "txHash",
		Short: "Run txHash backfiller",
		Run: func(_ *cobra.Command, _ []string) {
			txhash.Run()
		},
	}
	root.AddCommand(txHashFixFormatCommand)
}
