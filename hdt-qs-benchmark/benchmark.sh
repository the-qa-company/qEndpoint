#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=$(cat ./endpointurl.txt)

SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
OUTPUT=output

echo "(Re)create data dir..."

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

for SIZE in 10000 50000 100000
do
    # Clear the ENDPOINT
    if [ "$(curl "$UPDATE_URL" \
                --data-urlencode 'query=DELETE WHERE { ?s ?p ?o }' \
                -H 'accept: application/sparql-results+json' 2>/dev/null)" = "OK" ]
    then
        echo "Endpoint flushed"
    else 
        1>&2 echo "Can't clear the endpoint"
        exit -1
    fi

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

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done


