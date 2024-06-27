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

```bash
./db-queue-example dequeue -poll-interval 1s
```

