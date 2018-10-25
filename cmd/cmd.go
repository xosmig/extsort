package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/xosmig/extsort/extsort"
	sortio "github.com/xosmig/extsort/io"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "extsort [input_file] [output_file] [--ml memory_limit] [--text] [--bs buffer_size]",
	Short: "Sort numbers in text or binary format",
	Args: cobra.MaximumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		bufferSizeValues := bufferSize / sortio.SizeOfValue
		if bufferSizeValues < 1 {
			panic("TODO")
		}

		memoryLimitValues := memoryLimit / sortio.SizeOfValue
		if memoryLimitValues < 1 {
			panic("TODO4")
		}

		var input sortio.Uint64Reader
		var output sortio.Uint64Writer

		byteBuffer := sortio.NewUint64ByteBuf(bufferSizeValues)
		if len(args) >= 1 {
			f, err := os.Open(args[0])
			if err != nil {
				panic("TODO2")
			}
			defer f.Close()
			input = sortio.NewBinaryUint64ReaderCountBuf(f, bufferSizeValues, byteBuffer)
		} else {
			panic("Not implemented yet")
		}

		if len(args) >= 2 {
			f, err := os.Create(args[1])
			if err != nil {
				panic("TODO3")
			}
			defer f.Close()
			output = sortio.NewBinaryUint64WriterCountBuf(f, bufferSizeValues, byteBuffer)
		} else {
			panic("Not implemented yet")
		}

		params := extsort.DefaultParamsBufferSize(memoryLimitValues, bufferSizeValues)
		err := extsort.DoMultiwayMergeSortParams(input, output, params)
		if err != nil {
			panic("TODO5")
		}
	},
}

var memoryLimit int
var textFormat bool
var bufferSize int

func Execute() {
	rootCmd.PersistentFlags().IntVar(&memoryLimit, "ml", 1024 * 1024 * 1024, "memory limit (in bytes)")
	rootCmd.PersistentFlags().IntVar(&bufferSize, "bs", 8 * 4096, "buffer size (in bytes)")
	rootCmd.PersistentFlags().BoolVar(&textFormat, "text", false, "use textual format")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
