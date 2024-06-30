package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	tmpfile, err := os.OpenFile(fmt.Sprintf("/tmp/user_list_%s", e.ID), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer os.Remove(tmpfile.Name())
	for _, id := range ids {
		tmpfile.WriteString(id + "\n")
	}
	tmpfile.Close()

	commands := fmt.Sprintf("bin/process.sh %s %s", e.ID, tmpfile.Name())
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", commands)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}
