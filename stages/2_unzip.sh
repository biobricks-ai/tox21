#!/usr/bin/env bash

localpath=$(pwd)
echo "Local path: $localpath"

downloadpath="$localpath/download"
echo "Download path: $downloadpath"

temppath="$localpath/temp"
mkdir -p $temppath
echo "Temporal path: $temppath"

rawpath="$localpath/raw"
mkdir -p $rawpath
echo "Raw path: $rawpath"

cat $temppath/files.txt | tail -n +2 | xargs -P14 -n1 bash -c '
  filename="${1%.*}"
  echo '$downloadpath'/$1
  echo '$rawpath'/$filename
  unzip '$downloadpath'/$1 -d '$rawpath'/$filename
' {}