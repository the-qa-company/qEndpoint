#!/usr/bin/env bash


if (( $# < 1 )); then
    1>&2 echo "$0 (input_directory)"
    1>&2 echo "input_directory : directory to list the hashes"
    exit -1
fi
BASE=`dirname $0`
FILE=`realpath $1`

cd $BASE/..

for f in `ls $FILE` ; do
    echo "$f : $(sha256sum -b "$FILE/$f" | cut -d " " -f 1)"
done

