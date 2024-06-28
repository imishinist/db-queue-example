package main

import (
	"bufio"
	"encoding/json"
	"os"
)

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}

func readLineAsJson() ([]json.RawMessage, error) {
	scanner := bufio.NewScanner(os.Stdin)
	var lines []json.RawMessage
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		lines = append(lines, json.RawMessage(line))
	}
	return lines, nil
}
