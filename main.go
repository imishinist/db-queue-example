package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
)

type enqueueOpt struct {
}

func enqueueCmd() command {
	fs := flag.NewFlagSet("enqueue", flag.ExitOnError)
	opt := &enqueueOpt{}

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			fmt.Println("enqueue", opt)
			return nil
		},
	}
}

type dequeueOpt struct {
}

func dequeueCmd() command {
	fs := flag.NewFlagSet("dequeue", flag.ExitOnError)
	opt := &dequeueOpt{}

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return err
			}
			fmt.Println("dequeue", opt)
			return nil
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
