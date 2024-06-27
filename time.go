package main

import (
	"fmt"
	"time"
)

const defaultLayout = time.RFC3339

var expectedLayouts = []string{
	defaultLayout,
	time.RFC1123,
	time.RFC1123Z,
}

type TimeValue struct {
	Time *time.Time
}

func (v TimeValue) String() string {
	if v.Time != nil {
		return v.Time.Format(defaultLayout)
	}
	return ""
}

func (v TimeValue) Set(s string) error {
	success := false
	for _, layout := range expectedLayouts {
		t, err := time.Parse(layout, s)
		if err != nil {
			continue
		}
		*v.Time = t
		success = true
	}
	if !success {
		return fmt.Errorf("Failed to parse time from %s", s)
	}
	return nil
}
