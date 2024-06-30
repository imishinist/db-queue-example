#!/bin/bash

k=$1
user_id_file=$2

echo "[k=$k]: Reading user IDs from file: $user_id_file"
for user_id in $(cat $user_id_file); do
  echo "[k=$k]: User ID: $user_id"
  #sleep 1
done

