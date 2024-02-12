#!/bin/bash

size1=${1:-10k}
size2=${2:-20k}

typeset -i i=0

while (( i < 100 )); do
  outfile="out-${i}.json"
  url="http://localhost:8080/base64-1/file-${size1}.bin"
  echo -n $url
  curl --silent --request GET --output "${outfile}" "${url}"
  resultSize=$(wc -c ${outfile})
  rm ${outfile}
  echo " -> $resultSize"
  let i+=1
done

#while (( i < 100 )); do
#  outfile="out-${i}.json"
#  url="http://localhost:8080/base64-2/file-${size1}.bin/file-${size2}.bin"
#  echo -n $url
#  curl --silent --request GET --output "${outfile}" "${url}"
#  resultSize=$(wc -c ${outfile})
#  rm ${outfile}
#  echo " -> $resultSize"
#  let i+=1
#done