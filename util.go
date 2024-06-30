package main

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}

func chunkChBy[T any](input <-chan T, chunkSize int, waitTimeout time.Duration) <-chan []T {
	output := make(chan []T)

	timer := time.NewTimer(waitTimeout)
	go func() {
		defer close(output)
		defer timer.Stop()

		ret := make([]T, 0, chunkSize)
		for {
			select {
			case item, ok := <-input:
				if !ok {
					// closed
					if len(ret) > 0 {
						output <- ret
					}
					return
				}
				ret = append(ret, item)

				if len(ret) == chunkSize {
					output <- ret
					ret = make([]T, 0, chunkSize)
					timer.Reset(waitTimeout)
				}
			case <-timer.C:
				if len(ret) == 0 {
					timer.Reset(waitTimeout)
					continue
				}
				output <- ret
				ret = make([]T, 0, chunkSize)
				timer.Reset(waitTimeout)
			}
		}
	}()
	return output
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

func readLineAsJsonCh() <-chan json.RawMessage {
	ch := make(chan json.RawMessage)

	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}
			ch <- json.RawMessage(line)
		}
	}()
	return ch
}
