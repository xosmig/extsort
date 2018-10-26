package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/xosmig/extsort/extsort"
	sortio "github.com/xosmig/extsort/io"
	"github.com/xosmig/extsort/util"
	"io"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "extsort [input_file] [output_file]",
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

		var profiler = util.NewNilSimpleProfiler()
		if !disableProfiling {
			profiler = util.NewSimpleProfiler()
		}

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
		input.SetProfiler(profiler)

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
		output.SetProfiler(profiler)

		params := extsort.CreateParams(
			memoryLimitValues - 3 * bufferSizeValues,
			bufferSizeValues,
			useReplacementSelection)

		run := func() error { return extsort.DoMultiwayMergeSortParams(input, output, params, profiler) }
		if noSort {
			run = func() error { return sortio.CopyValues(input, output) }
		}


		profiler.Start()
		err := run()
		profiler.Finish()

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if !profiler.IsNilProfiler() {
			fmt.Fprint(os.Stderr, "Profiling results:\n")
			fmt.Fprintf(os.Stderr, "io time: %.2f seconds\n", float64(profiler.GetTotalMeasuredDuration().Nanoseconds()) / 1e9)
			fmt.Fprintf(os.Stderr, "total time: %.2f seconds\n", float64(profiler.GetTotalRunningDuration().Nanoseconds()) / 1e9)
			fmt.Fprintf(os.Stderr, "io time ratio: %.2f\n", profiler.GetMeasuredDurationRatio())
		}
	},
}

var memoryLimit int
var textFormat bool
var textInputFormat bool
var textOutputFormat bool
var useReplacementSelection bool
var noSort bool
var bufferSize int
var disableProfiling bool

func Execute() {
	rootCmd.PersistentFlags().IntVar(&memoryLimit, "ml", 1024 * 1024 * 1024, "Memory limit (in bytes)")
	rootCmd.PersistentFlags().IntVar(&bufferSize, "bs", 8 * 4096, "Buffer size (in bytes)")
	rootCmd.PersistentFlags().BoolVar(&textFormat, "text", false, "Use textual format")
	rootCmd.PersistentFlags().BoolVar(&textInputFormat, "text_input", false, "Use textual input format")
	rootCmd.PersistentFlags().BoolVar(&textOutputFormat, "text_output", false, "Use textual output format")
	rootCmd.PersistentFlags().BoolVar(&useReplacementSelection, "replacement_selection",
		false, "Use replacement selection algorithm")
	rootCmd.PersistentFlags().BoolVar(&disableProfiling, "no_prof", false, "disable io profiling")
	rootCmd.PersistentFlags().BoolVar(&noSort, "no_sort",
		false, "Just output the input data without sorting. Can be used to convert from one format to another.")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
