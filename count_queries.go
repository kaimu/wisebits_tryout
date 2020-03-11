package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
)

const scanBufferSize = bufio.MaxScanTokenSize

// partSavingFunc saves a part of analyzed (mapped) data for further use
type partSavingFunc func(map[string]int, uint64) error

// partReadWritingFunc returns a way to write/read/close part data, which is supposed to be gob-marshaled
type serializedPartReadWritingFunc func() (ReadWriteClearCloser, error)

type ReadWriteClearCloser interface {
	io.ReadWriteCloser
	Clear() error
}

// dataToParts reads the data and saves it by parts with the specified part-size limit.
// The first part number passed to `save` is 1, not 0
func dataToParts(r io.Reader, limitPerPart int, save partSavingFunc) (partsCount uint64, err error) {
	part := map[string]int{}
	scanner := bufio.NewScanner(r)
	// The next two rows explicitly included just for tuning, so we could replace `scanBufferSize`
	// with a value that fits us best in a real world scenario
	buf := make([]byte, scanBufferSize)
	scanner.Buffer(buf, scanBufferSize)
	// To keep things simple, read by line (default Scanner Split function)
	for scanner.Scan() {
		if limitPerPart > 0 {
			// With a zero or negative limit we probably should save empty parts,
			// otherwise populate the current part with a new line
			//part = append(part, scanner.Text())
			part[scanner.Text()] += 1
		}
		if len(part) >= limitPerPart {
			// Limit reached, save the current part
			partsCount += 1
			err = save(part, partsCount)
			if err != nil {
				return
			}
			// The current part is saved, start a new one
			part = map[string]int{}
		}
	}
	err = scanner.Err()
	if err != nil {
		// Non-EOF error occurred, abort
		return
	}
	// EOF, save the last part (if not empty)
	if len(part) > 0 {
		partsCount += 1
		err = save(part, partsCount)
	}
	return
}

// reduceSerializedParts expects a list of functions to read/write serialized parts data.
// It might seem as an overcomplication (why not just pass a list of ReadWriters instead
// of factory functions?), but when working with files it solves open-file-limit problem
// on Linux, as each file is opened only on factory function call and closed right after
func reduceSerializedParts(parts []serializedPartReadWritingFunc, result io.Writer) error {
	for i := 0; i < len(parts); i++ {
		// Read the part to add the values from other parts to its counters
		resultingPartRWC, err := parts[i]()
		if err != nil {
			return err
		}
		resultingPart, err := deserializeMap(resultingPartRWC)
		if err != nil {
			return err
		}
		err = resultingPartRWC.Close()
		if err != nil {
			return err
		}
		// Update counters using corresponding values from remaining parts
		for o := i + 1; o < len(parts); o++ {
			otherPartRWC, err := parts[o]()
			if err != nil {
				return err
			}
			otherPart, err := deserializeMap(otherPartRWC)
			if err != nil {
				otherPartRWC.Close()
				return err
			}
			for k := range resultingPart {
				resultingPart[k] += otherPart[k]
				// Filter out the transferred values
				delete(otherPart, k)
			}
			// Save the used part after the changes
			err = otherPartRWC.Clear()
			if err != nil {
				return err
			}
			err = serializeMap(otherPart, otherPartRWC)
			otherPartRWC.Close()
			if err != nil {
				return err
			}
		}
		// Write to the result as tab-delimited lines
		err = writeMapTabDelim(resultingPart, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func serializeMap(part map[string]int, result io.Writer) error {
	enc := gob.NewEncoder(result)
	return enc.Encode(part)
}

func deserializeMap(r io.Reader) (result map[string]int, err error) {
	enc := gob.NewDecoder(r)
	err = enc.Decode(&result)
	return
}

func writeMapTabDelim(m map[string]int, result io.Writer) (err error) {
	for k, v := range m {
		line := fmt.Sprintf("%s\t%d\n", k, v)
		_, err = result.Write([]byte(line))
		if err != nil {
			return
		}
	}
	return nil
}
