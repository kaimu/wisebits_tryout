package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/pkg/profile"
)

type CommandLineArgs struct {
	limit  int
	input  string
	output string
}

var memprofile = flag.String("memprofile", "", "папка для файла mem.pprof")

func main() {
	clArgs := parseCommandLineArgs()
	if err := validateArgs(clArgs); err != nil {
		log.Fatalln(err)
	}
	if err := countQueries(clArgs); err != nil {
		log.Fatalln(err)
	}
}

func parseCommandLineArgs() CommandLineArgs {
	res := CommandLineArgs{}
	flag.IntVar(&res.limit, "limit", 950, "лимит на размер map в диапазоне от 150 до 950")
	flag.StringVar(&res.input, "input", "", "путь к файлу с данными для анализа")
	flag.StringVar(&res.output, "output", "", "путь к результирующему файлу")
	flag.Parse()
	return res
}

func validateArgs(args CommandLineArgs) error {
	if strings.TrimSpace(args.input) == "" || strings.TrimSpace(args.output) == "" {
		return errors.New("Необходимо указать путь к файлу с данными и результирующему файлу")
	}
	if args.limit < 150 || args.limit > 950 {
		return errors.New("Лимит должен быть в диапазоне от 150 до 950")
	}
	return nil
}

func countQueries(args CommandLineArgs) (err error) {
	if *memprofile != "" {
		// Profiling
		defer profile.Start(profile.MemProfile, profile.ProfilePath(*memprofile), profile.MemProfileRate(1)).Stop()
	}
	inputFile, err := os.Open(args.input)
	if err != nil {
		return
	}
	defer inputFile.Close()
	f, tmpDir, err := tmpFilePartSerializedSavingFactory()
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir) // Clean up
	// 1. Split input data to intermediate mapped-parts
	partsCount, err := dataToParts(inputFile, args.limit, f)
	// 2. Create a list of function to read/write to it
	var partsRW []serializedPartReadWritingFunc
	for i := uint64(1); i <= partsCount; i++ {
		partsRW = append(partsRW, tmpFilePartSerializedReadWritingFactory(tmpDir, i))
	}
	// 3. Create a file to write to
	outputFile, err := os.Create(args.output)
	if err != nil {
		return
	}
	// 4. Reduce parts data to a single resulting file
	return reduceSerializedParts(partsRW, outputFile)
}

// Creates a temporary dir inside system temp dir, and returns a function to write
// binary-marshaled (gob) parts of the analyzed (mapped) data to it
func tmpFilePartSerializedSavingFactory() (result partSavingFunc, dir string, err error) {
	dir, err = ioutil.TempDir(os.TempDir(), "count-queries")
	if err != nil {
		return
	}
	result = func(part map[string]int, partNum uint64) (err error) {
		// Open a new temporary file and write to it
		tmpFile, err := os.Create(fmt.Sprintf("%s/part%d.bin", dir, partNum))
		if err != nil {
			return
		}
		defer tmpFile.Close()
		err = serializeMap(part, tmpFile)
		return
	}
	return
}

// Returns a function to read/write/close serialized parts from temp-files.
func tmpFilePartSerializedReadWritingFactory(tmpDir string, partNum uint64) serializedPartReadWritingFunc {
	return func() (rwc ReadWriteClearCloser, err error) {
		// Read/write Mode
		file, err := os.OpenFile(fmt.Sprintf("%s/part%d.bin", tmpDir, partNum), os.O_RDWR, 0666)
		if err != nil {
			return
		}
		w := fileWrapper{file}
		rwc = &w
		return
	}
}

type fileWrapper struct {
	*os.File
}

func (file *fileWrapper) Clear() (err error) {
	err = file.Truncate(0)
	if err != nil {
		return
	}
	_, err = file.Seek(0, 0)
	return
}
