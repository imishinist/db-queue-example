# Postgres QUEUE example

Consumers connect to read-only replicas and producers connect to the master.

## Setup

```bash
docker compose up -d --build
go build
```

## run

### Enqueue

```bash
echo '{"user_id": 1}' | ./db-queue-example enqueue
```


### Dequeue

It consumes from read-replica.

```bash
./db-queue-example dequeue -dsn 'postgres://postgres:password@localhost:5433?sslmode=disable' -poll-interval 1s
```

