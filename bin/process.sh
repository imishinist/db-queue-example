#!/bin/bash

k=$1
user_id_file=$2

trap "rm $user_id_file" EXIT

echo "[k=$k]: Reading user IDs from file: $user_id_file"
for user_id in $(cat $user_id_file); do
  echo "[k=$k]: User ID: $user_id"
  sleep 10
done
