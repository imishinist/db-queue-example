package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
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
			broker := NewBroker(conn)

			lines, err := readLineAsJson()
			if err != nil {
				return err
			}
			chunks := chunkBy(lines, opt.BatchSize)
			for i, chunk := range chunks {
				if i > 0 {
					time.Sleep(opt.BatchInterval)
				}
				records, err := broker.Produce(ctx, chunk, opt.RecordDelay)
				if err != nil {
					return err
				}
				for _, record := range records {
					json.NewEncoder(os.Stdout).Encode(record)
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
			broker := NewBroker(conn)
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

type enqueueSQSOpt struct {
	EndpointURL string
	QueueName   string
}

func SQSConnect(endpointURL string) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-1"))
	if err != nil {
		return nil, err
	}

	svc := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})
	return svc, nil
}

func CreateQueue(svc *sqs.Client, queueName string) (string, error) {
	res, err := svc.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	return *res.QueueUrl, nil
}

func enqueueSQSCmd() command {
	fs := flag.NewFlagSet("enqueue-sqs", flag.ExitOnError)
	opt := &enqueueSQSOpt{
		EndpointURL: "http://localhost:9324",
		QueueName:   "default",
	}
	fs.StringVar(&opt.EndpointURL, "endpoint-url", opt.EndpointURL, "endpoint url")
	fs.StringVar(&opt.QueueName, "queue-name", opt.QueueName, "queue name")

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return nil
			}

			svc, err := SQSConnect(opt.EndpointURL)
			if err != nil {
				return err
			}
			queueURL, err := CreateQueue(svc, opt.QueueName)
			if err != nil {
				return err
			}

			lineCh := readLineAsJsonCh()
			for chunk := range chunkChBy(lineCh, 10) {
				messages := make([]types.SendMessageBatchRequestEntry, 0)
				for i, line := range chunk {
					messages = append(messages, types.SendMessageBatchRequestEntry{
						Id:          aws.String(strconv.Itoa(i)),
						MessageBody: aws.String(string(line)),
					})
				}
				res, err := svc.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
					QueueUrl: aws.String(queueURL),
					Entries:  messages,
				})
				if err != nil {
					return err
				}
				if res.Failed != nil {
					json.NewEncoder(os.Stdout).Encode(res.Failed)
				}
				if res.Successful != nil {
					json.NewEncoder(os.Stdout).Encode(res.Successful)
				}
			}

			return nil
		},
	}
}

type dequeueSQSOpt struct {
	EndpointURL string
	QueueName   string

	PollInterval time.Duration
}

func dequeueSQSCmd() command {
	fs := flag.NewFlagSet("dequeue-sqs", flag.ExitOnError)
	opt := &dequeueSQSOpt{
		EndpointURL:  "http://localhost:9324",
		QueueName:    "default",
		PollInterval: 1 * time.Second,
	}
	fs.StringVar(&opt.EndpointURL, "endpoint-url", opt.EndpointURL, "endpoint url")
	fs.StringVar(&opt.QueueName, "queue-name", opt.QueueName, "queue name")
	fs.DurationVar(&opt.PollInterval, "poll-interval", opt.PollInterval, "poll interval")

	return command{
		fs: fs,
		fn: func(args []string) error {
			if err := fs.Parse(args); err != nil {
				return nil
			}
			svc, err := SQSConnect(opt.EndpointURL)
			if err != nil {
				return err
			}
			queueURL, err := CreateQueue(svc, opt.QueueName)
			if err != nil {
				return err
			}
			for {
				res, err := svc.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(queueURL),
					MaxNumberOfMessages: 10,
					WaitTimeSeconds:     20,
				})
				if err != nil {
					return err
				}
				if len(res.Messages) > 0 {
					deletes := make([]types.DeleteMessageBatchRequestEntry, 0, len(res.Messages))
					for id, msg := range res.Messages {
						json.NewEncoder(os.Stdout).Encode(msg)

						deletes = append(deletes, types.DeleteMessageBatchRequestEntry{
							Id:            aws.String(strconv.Itoa(id)),
							ReceiptHandle: msg.ReceiptHandle,
						})
					}
					res, err := svc.DeleteMessageBatch(context.TODO(), &sqs.DeleteMessageBatchInput{
						QueueUrl: aws.String(queueURL),
						Entries:  deletes,
					})
					if err != nil {
						return err
					}
					if res.Failed != nil {
						json.NewEncoder(os.Stdout).Encode(res.Failed)
					}
				} else {
					time.Sleep(opt.PollInterval)
				}
			}
		},
	}
}

func main() {
	commands := map[string]command{
		"enqueue":     enqueueCmd(),
		"dequeue":     dequeueCmd(),
		"enqueue-sqs": enqueueSQSCmd(),
		"dequeue-sqs": dequeueSQSCmd(),
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
