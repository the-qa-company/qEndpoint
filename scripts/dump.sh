#!/usr/bin/env bash

#Location of the qendpoint directory
STORE_LOCATION=qendpoint
#Location of the dump directory
DUMP_LOCATION=dump
#Output location
OUTPUT_DIR=latest_dump
#HDT filename
INDEX_NAME=index_dev.hdt

# Sleep between the calls
SLEEP_TIME=2

echo "Waiting for the end of the dump"
if ( ! curl 'http://localhost:1234/api/endpoint/dump' > /dev/null ); then
  echo "Can't send dump command, abort"
  exit -1
fi

# Wait for the dump to start
echo "Waiting for the end of the dump"
sleep $SLEEP_TIME

while [ true ]; do
    res=$(curl 'http://localhost:1234/api/endpoint/is_dumping')
    case $res in
        *true* )
             sleep $SLEEP_TIME
             ;;
        *false* )
            break
             ;;
    esac
done

echo "Dump done"

# Using the last dump for the restore

# get the latest dump
dump_name=$(ls "$DUMP_LOCATION" -r | head -n 1)
dump_dir="$DUMP_LOCATION/$dump_name"

# Clear old dump and copy new one
echo "Moving '$dump_dir' to '$OUTPUT_DIR'"
rm -rf "$OUTPUT_DIR"
mv "$dump_dir" "$OUTPUT_DIR"


# Copy repo files
cp "$STORE_LOCATION/prefixes.sparql" "$OUTPUT_DIR"
cp "$STORE_LOCATION/repo_model.ttl" "$OUTPUT_DIR"

# HDT store
mkdir -p "$OUTPUT_DIR/hdt-store"
mv "$OUTPUT_DIR/store.hdt" "$OUTPUT_DIR/hdt-store/$INDEX_NAME"

# Indexes
mkdir -p "$OUTPUT_DIR/native-store"
mv "$OUTPUT_DIR/lucene" "$OUTPUT_DIR/native-store"
