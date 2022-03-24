#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=http://127.0.0.1:1234/api/endpoint

SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load
OUTPUT=output
RESULTS=results

echo "(Re)create result dir..."
mkdir -p "$RESULTS"

echo "Downloading BSBM..."
# Download the tool to generate the file
if [ ! -f "bsbmtools" ]
then
    echo "bsbmtools already installed, to delete it run 'rm -r bsbmtools'"
else 
    curl https://phoenixnap.dl.sourceforge.net/project/bsbmtools/bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip --output bsmtools.zip
    unzip bsmtools.zip
    rm bsmtools.zip
    mv bsbmtools-0.2 bsbmtools
fi

# set into bsbm tool suite
cd bsbmtools

OUTPUT_IN=../$OUTPUT

for SIZE in 10000 50000 100000 500000
do
    echo "Generating $OUTPUT/dataset$SIZE..."
    # Remove previous dataset
    rm -rf "$OUTPUT_IN"
    mkdir -p "$OUTPUT_IN"

    # Generate the dataset
    ./generate \
                -s nt \
                -pc $SIZE \
                -dir "$OUTPUT_IN/data$SIZE" \
                -fn "$OUTPUT_IN/dataset$SIZE" \
    | tail -n 1

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else 
        1>&2 echo "Can't load the NT file"
        exit -1
    fi


    # Test the dataset
    if ! ./testdriver \
                      -ucf usecases/explore/sparql.txt \
                      -u "$UPDATE_URL" \
                      -udataset "$OUTPUT_IN/dataset$SIZE.nt" \
                      -idir "$OUTPUT_IN/data$SIZE" \
                      "$SPARQL_URL"
    then
        1>&2 echo "Can't test dataset$SIZE"
        exit -1
    fi

    mv "benchmark_result.xml" "../$RESULTS/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done


