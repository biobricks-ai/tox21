#!/usr/bin/env bash
# unzip files to ./raw

localpath=$(pwd)
echo "Local path: $localpath"

downloadpath="$localpath/download"
echo "Download path: $downloadpath"

temppath="$localpath/temp"
mkdir -p $temppath
echo "Temporal path: $temppath"

rawpath="$localpath/raw"
# delete rawpath if it exists
if [ -d "$rawpath" ]; then
  rm -rf $rawpath
fi
mkdir -p $rawpath
echo "Raw path: $rawpath"

cat $temppath/files.txt | tail -n +2 | xargs -P14 -n1 bash -c '
  filename="${1%.*}"
  #echo '$downloadpath'/$1
  echo '$rawpath'/$filename
  unzip -q '$downloadpath'/$1 -d '$rawpath'/$filename
' {}

for x in $rawpath/*/; do
  echo $x
  for y in $x*; do
    echo $y
    y=$(realpath --relative-to="$localpath" "$y")
    z=$(printf %s "$y" | tr "-" "_")
    echo $z
    mv $y $z
  done
done