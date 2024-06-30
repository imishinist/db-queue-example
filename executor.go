package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/google/uuid"
)

type Executor struct {
	ID string
}

func NewExecutor(id string) *Executor {
	return &Executor{
		ID: id,
	}
}

func (e *Executor) Run(ctx context.Context, ids []string) error {
	tmpfile, err := os.OpenFile(fmt.Sprintf("/tmp/%s", uuid.New().String()), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	for _, id := range ids {
		tmpfile.WriteString(id + "\n")
	}
	tmpfile.Close()

	commands := fmt.Sprintf("bin/process.sh %s %s >>/tmp/%s.stdout 2>>/tmp/%s.stderr", e.ID, tmpfile.Name(), e.ID, e.ID)
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", commands)
	if err := cmd.Start(); err != nil {
		return err
	}

	return nil
}
