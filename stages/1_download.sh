#!/usr/bin/env bash

# Download files

localpath=$(pwd)
echo "Local path: $localpath"

temppath="$localpath/temp"
echo "Temporal path: $temppath"
mkdir -p $temppath
cd $temppath;

ftpbase="https://opendata.ncats.nih.gov/public/tox21/assay/data/"
wget --no-remove-listing $ftpbase
cat index.html | grep -Po '(?<=href=")[^"]*' | sort | cut -d "/" -f 10 > files.txt
rm .listing
rm index.html

downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd $downloadpath;

cat $temppath/files.txt | xargs -P14 -n1 bash -c '
# echo $1
newfilename=$(echo $1 | tr - _)
echo $newfilename
echo $newfilename >> '$temppath'/newfiles.txt
wget -nH -q -nc -P '$downloadpath' '$ftpbase'$1 -O $newfilename' {}

rm $temppath/files.txt
mv $temppath/newfiles.txt $temppath/files.txt

echo "Download done."
