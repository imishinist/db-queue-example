package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"time"
)

type enqueueOpt struct {
	BatchSize     int
	BatchInterval time.Duration

	RecordDelay time.Duration

	DSN string
}

func enqueueCmd() command {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	opt := &enqueueOpt{
		BatchSize:     10,
		BatchInterval: 1 * time.Second,
		RecordDelay:   1 * time.Second,
		DSN:           "postgres://postgres:password@localhost:5432?sslmode=disable",
	}
	fs.IntVar(&opt.BatchSize, "batch-size", opt.BatchSize, "batch size")
	fs.DurationVar(&opt.BatchInterval, "batch-interval", opt.BatchInterval, "batch interval")
	fs.DurationVar(&opt.RecordDelay, "record-delay", opt.RecordDelay, "record delay")
	fs.StringVar(&opt.DSN, "dsn", opt.DSN, "database source name")

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			conn, err := DBConnect(opt.DSN)
			if err != nil {
				return err
			}

			ctx := context.Background()
			broker := NewBroker(conn, opt.RecordDelay)

			lines, err := readLineAsJson()
			if err != nil {
				return err
			}
			chunks := chunkBy(lines, opt.BatchSize)
			for i, chunk := range chunks {
				if i > 0 {
					time.Sleep(opt.BatchInterval)
				}
				if err := broker.Produce(ctx, chunk); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

type dequeueOpt struct {
	BatchSize    int
	PollInterval time.Duration

	LastVT time.Time

	DSN string
}

func dequeueCmd() command {
	fs := flag.NewFlagSet("dequeue", flag.ExitOnError)
	opt := &dequeueOpt{
		BatchSize:    10,
		PollInterval: 5 * time.Second,
		DSN:          "postgres://postgres:password@localhost:5432?sslmode=disable",
	}
	fs.IntVar(&opt.BatchSize, "batch-size", opt.BatchSize, "batch size")
	fs.DurationVar(&opt.PollInterval, "poll-interval", opt.PollInterval, "poll interval")
	fs.Var(&TimeValue{Time: &opt.LastVT}, "last-vt", "last vt")
	fs.StringVar(&opt.DSN, "dsn", opt.DSN, "database source name")

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			conn, err := DBConnect(opt.DSN)
			if err != nil {
				return err
			}

			ctx := context.Background()
			broker := NewBroker(conn, time.Duration(0))
			broker.SetCursor(opt.LastVT)

			for {
				records, err := broker.Consume(ctx, opt.BatchSize)
				if err != nil {
					return err
				}
				for _, record := range records {
					json.NewEncoder(os.Stdout).Encode(record)
				}
				if len(records) > 0 {
					last := broker.GetCursor()
					log.Printf("current vt: %s", last.Format(time.RFC3339Nano))
				}
				time.Sleep(opt.PollInterval)
			}
		},
	}
}

func main() {
	commands := map[string]command{
		"enqueue": enqueueCmd(),
		"dequeue": dequeueCmd(),
	}

	fs := flag.NewFlagSet("db-queue", flag.ExitOnError)

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: db-queue <command> [arguments]")
		fmt.Fprintf(fs.Output(), "\n global flags: \n")
		fs.PrintDefaults()

		names := make([]string, 0, len(commands))
		for name := range commands {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			if cmd := commands[name]; cmd.fs != nil {
				fmt.Fprintf(fs.Output(), "\n%s command: \n", name)
				cmd.fs.SetOutput(fs.Output())
				cmd.fs.PrintDefaults()
			}
		}
	}
	fs.Parse(os.Args[1:])

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		os.Exit(1)
	}

	cmd, ok := commands[args[0]]
	if !ok {
		log.Fatalf("Unknown command: %s", args[0])
	}
	if err := cmd.fn(args[1:]); err != nil {
		log.Fatal(err)
	}
}

type command struct {
	fs *flag.FlagSet
	fn func(args []string) error
}
