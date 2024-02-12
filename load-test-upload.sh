#!/bin/bash

size=${1:-10k}

typeset -i i=0

while (( i < 100 )); do
  curl --request POST \
    --header "Content-Type: text/plain" \
    --data-binary @FILES/file-${size}.bin \
    "http://localhost:8080/file/test-${size}-${i}.txt"
  let i+=1
  echo
done
