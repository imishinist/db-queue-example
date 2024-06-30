#!/bin/bash

user_id_file=$1

echo "Reading user IDs from file: $user_id_file"
for user_id in $(cat $user_id_file); do
  echo "User ID: $user_id"
  sleep 1
done

