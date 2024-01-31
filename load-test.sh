#!/bin/bash

size=${1:-10k}

typeset -i i=0

#while (( i < 100 )); do
#  curl --request POST \
#    --header "Content-Type: text/plain" \
#    --data-binary @FILES/file-${size}.bin \
#    "http://localhost:8080/file/test-${size}-${i}.txt"
#  let i+=1
#  echo
#done

let i=0

while (( i < 100 )); do
  outfile="OUT/out-${i}.json"
  url="http://localhost:8080/base64/file-${size}.bin"
  echo -n $url
  curl --silent --request GET --output "${outfile}" "${url}"
  resultSize=$(wc -c ${outfile})
  rm ${outfile}
  echo " -> $resultSize"
  let i+=1
done