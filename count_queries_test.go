package main

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestDataToParts(t *testing.T) {
	data := `qA
qB
qC
qD
qA
qA
qE
qF
`

	// Max 5 per part
	r := strings.NewReader(data)
	parts := []map[string]int{}
	partsCount, _ := dataToParts(r, 5, slicePartSavingFactory(&parts))
	expected := []map[string]int{
		{"qA": 3, "qB": 1, "qC": 1, "qD": 1, "qE": 1},
		{"qF": 1},
	}
	if !reflect.DeepEqual(parts, expected) {
		t.Errorf("Incorrect data parts, got: %v, want: %v.", parts, expected)
	}
	if partsCount != 2 {
		t.Errorf("Incorrect data parts count, got: %v, want: %v.", partsCount, 2)
	}

	// Max 0
	r.Reset(data)
	parts = []map[string]int{}
	partsCount, _ = dataToParts(r, 0, slicePartSavingFactory(&parts))
	expected = []map[string]int{
		{}, {}, {}, {}, {}, {}, {}, {},
	}
	if !reflect.DeepEqual(parts, expected) {
		t.Errorf("Incorrect data parts, got: %v, want: %v.", parts, expected)
	}
	if partsCount != 8 {
		t.Errorf("Incorrect data parts count, got: %v, want: %v.", partsCount, 8)
	}

	// Max 1
	r.Reset(data)
	parts = []map[string]int{}
	_, _ = dataToParts(r, 1, slicePartSavingFactory(&parts))
	expected = []map[string]int{
		{"qA": 1}, {"qB": 1}, {"qC": 1}, {"qD": 1}, {"qA": 1}, {"qA": 1}, {"qE": 1}, {"qF": 1},
	}
	if !reflect.DeepEqual(parts, expected) {
		t.Errorf("Incorrect data parts, got: %v, want: %v.", parts, expected)
	}

	// Max 10
	r.Reset(data)
	parts = []map[string]int{}
	_, _ = dataToParts(r, 10, slicePartSavingFactory(&parts))
	expected = []map[string]int{
		{"qA": 3, "qB": 1, "qC": 1, "qD": 1, "qE": 1, "qF": 1},
	}
	if !reflect.DeepEqual(parts, expected) {
		t.Errorf("Incorrect data parts, got: %v, want: %v.", parts, expected)
	}
}

func slicePartSavingFactory(result *[]map[string]int) partSavingFunc {
	// Saves the new part to a provided slice in memory
	return func(part map[string]int, _ uint64) (_ error) {
		*result = append(*result, part)
		return
	}
}

type testPartBuffer struct {
	bytes.Buffer
}

func (b *testPartBuffer) Close() error {
	return nil
}

func (b *testPartBuffer) Clear() error {
	return nil
}

func TestPartsReduce(t *testing.T) {
	parts := []map[string]int{
		{"qA": 2, "qB": 1, "qC": 1, "qD": 1},
		{"qA": 1, "qE": 1},
		{"qC": 4},
	}

	var partsRW []serializedPartReadWritingFunc
	for _, part := range parts {
		// Create list of factory functions that use in-memory buffer for each part
		var serialized testPartBuffer
		_ = serializeMap(part, &serialized)
		partsRW = append(partsRW, func() (rwc ReadWriteClearCloser, err error) {
			rwc = &serialized
			return
		})
	}
	expected := `qA	3
qB	1
qC	5
qD	1
qE	1
`
	result := bytes.NewBufferString("")
	_ = reduceSerializedParts(partsRW, result)

	// We need to split into lines and sort them first before comparision, because `reduceSerializedParts`
	// does not care about the order (the data comes from map)
	resultLines := strings.Split(result.String(), "\n")
	sort.Strings(resultLines)
	expectedLines := strings.Split(expected, "\n")
	sort.Strings(expectedLines)

	if !reflect.DeepEqual(resultLines, expectedLines) {
		t.Errorf("Incorrect reduced parts result, got: %v, want: %v.", result.String(), expected)
	}
}
