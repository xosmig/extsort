package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/xosmig/extsort/extsort"
	sortio "github.com/xosmig/extsort/io"
	"io"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "extsort [input_file] [output_file] [--ml memory_limit] [--text] [--bs buffer_size]",
	Short: "Sort numbers in text or binary format",
	Args: cobra.MaximumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		bufferSizeValues := bufferSize / sortio.SizeOfValue
		if bufferSizeValues < 1 {
			fmt.Fprintln(os.Stderr, "Too small buffer size")
			os.Exit(2)
		}

		memoryLimitValues := memoryLimit / sortio.SizeOfValue
		if memoryLimitValues < 1 {
			fmt.Fprintln(os.Stderr, "Too small memory limit")
			os.Exit(2)
		}

		byteBuffer := sortio.NewUint64ByteBuf(bufferSizeValues)

		var inputFile io.Reader
		if len(args) >= 1 {
			f, err := os.Open(args[0])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
				os.Exit(1)
			}
			defer f.Close()
			inputFile = f
		} else {
			inputFile = os.Stdin
		}

		var input sortio.Uint64Reader
		if textFormat || textInputFormat {
			input = sortio.NewTextUint64ReaderCount(inputFile, bufferSizeValues)
		} else {
			input = sortio.NewBinaryUint64ReaderCountBuf(inputFile, bufferSizeValues, byteBuffer)
		}

		var outputFile io.Writer
		if len(args) >= 2 {
			f, err := os.Create(args[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening output file: %v\n", err)
				os.Exit(1)
			}
			defer f.Close()
			outputFile = f
		} else {
			outputFile = os.Stdout
		}

		var output sortio.Uint64Writer
		if textFormat || textOutputFormat {
			output = sortio.NewTextUint64WriterCount(outputFile, bufferSizeValues)
		} else {
			output = sortio.NewBinaryUint64WriterCountBuf(outputFile, bufferSizeValues, byteBuffer)
		}

		params := extsort.CreateParams(
			memoryLimitValues - 3 * bufferSizeValues,
			bufferSizeValues,
			useReplacementSelection)

		err := extsort.DoMultiwayMergeSortParams(input, output, params)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sorting data: %v\n", err)
			os.Exit(1)
		}
	},
}

var memoryLimit int
var textFormat bool
var textInputFormat bool
var textOutputFormat bool
var useReplacementSelection bool
var bufferSize int

func Execute() {
	rootCmd.PersistentFlags().IntVar(&memoryLimit, "ml", 1024 * 1024 * 1024, "memory limit (in bytes)")
	rootCmd.PersistentFlags().IntVar(&bufferSize, "bs", 8 * 4096, "buffer size (in bytes)")
	rootCmd.PersistentFlags().BoolVar(&textFormat, "text", false, "use textual format")
	rootCmd.PersistentFlags().BoolVar(&textInputFormat, "text_input", false, "use textual input format")
	rootCmd.PersistentFlags().BoolVar(&textOutputFormat, "text_output", false, "use textual output format")
	rootCmd.PersistentFlags().BoolVar(&useReplacementSelection, "replacement_selection",
		false, "use replacement selection algorithm")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
