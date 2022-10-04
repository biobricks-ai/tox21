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

brickpath="$localpath/brick"
mkdir -p $brickpath
echo "Brick path: $brickpath"

cat $temppath/files.txt | tail -n +4 | xargs -P14 -n1 bash -c '
  filename="${1%.*}"
  echo '$rawpath'/$filename/$filename.txt
  echo '$brickpath'/$filename.parquet
  python stages/tsv2parquet.py '$rawpath'/$filename/$filename.txt '$brickpath'/$filename.parquet
  python stages/tsv2parquet.py '$rawpath'/$filename/$filename.aggregrated.txt '$brickpath'/$filename.aggregrated.parquet
' {}