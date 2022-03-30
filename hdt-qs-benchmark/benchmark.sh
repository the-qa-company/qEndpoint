#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=http://127.0.0.1:1236/api/endpoint

SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load
OUTPUT=output
RUN=run
RESULTS=results
JAVA_MAX_MEM=6G
RUN_HS=false
RUN_NS=true
CSV=$RESULTS/results.csv

trap "echo 'EXITING...'; exit -1" INT
trap "echo 'DELETE $RUN $OUTPUT' ; rm -rf $RUN $OUTPUT" EXIT

#./build_endpoint.sh $RUN
mkdir -p $RUN
cp endpoint.jar $RUN

echo "(Re)create result dir..."
mkdir -p "$RESULTS"
mkdir -p "$RESULTS/nativestore"
mkdir -p "$RESULTS/hybridstore"

touch "$CSV"

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

OUTPUT_IN=../$OUTPUT
CSV_IN=../$RESULTS/results.csv
cd bsbmtools

if $RUN_HS
then

echo ""
echo "---------------------------------"
echo "--- RUN HYBRIDSTORE BENCHMARK ---"
echo "---------------------------------"
echo ""

cd ..
cp application.properties "$RUN/application.properties"

echo "repoModel=../models/model_hs.ttl" >> "$RUN/application.properties"

cd $RUN
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &

HDT_EP_PID=$!
trap "echo 'killing HDT EP';kill -KILL $HDT_EP_PID" EXIT INT

cd ..
cd bsbmtools
for SIZE in 10000 50000 100000 200000
do
    echo "Generating $OUTPUT/dataset$SIZE..."
    # Remove previous dataset
    rm -rf "$OUTPUT_IN"
    mkdir -p "$OUTPUT_IN"

    # Generate the dataset
    TRIPLES_COUNT=$(./generate \
                -s nt \
                -pc $SIZE \
                -dir "$OUTPUT_IN/data$SIZE" \
                -fn "$OUTPUT_IN/dataset$SIZE" \
    | tail -n 1 | cut -w -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    HDT_SIZE=$(du run/hdt-store | tail -n 1 | cut -w -f 1)
    NS_SIZE=$(du run/hdt-store | tail -n 1 | cut -w -f 1)

    echo "HYBRID,$SIZE,$TRIPLES_COUNT,$HDT_SIZE,$NS_SIZE" >> CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"

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

    mv "benchmark_result.xml" "../$RESULTS/hybridstore/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done

echo "kill HDT ENDPOINT"

kill -KILL $HDT_EP_PID

fi

if $RUN_NS
then

echo ""
echo "---------------------------------"
echo "--- RUN NATIVESTORE BENCHMARK ---"
echo "---------------------------------"
echo ""

cd ..

cp application.properties "$RUN/application.properties"

echo "repoModel=../models/model_ns.ttl" >> "$RUN/application.properties"

cd $RUN
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &

NATIVE_EP_PID=$!
trap "echo 'killing NS EP';kill -KILL $NATIVE_EP_PID" EXIT INT

cd ..

cd bsbmtools

for SIZE in 10000 50000 100000 200000
do
    echo "Generating $OUTPUT/dataset$SIZE..."
    # Remove previous dataset
    rm -rf "$OUTPUT_IN"
    mkdir -p "$OUTPUT_IN"

    # Generate the dataset
    TRIPLES_COUNT=$(./generate \
                -s nt \
                -pc $SIZE \
                -dir "$OUTPUT_IN/data$SIZE" \
                -fn "$OUTPUT_IN/dataset$SIZE" \
    | tail -n 1 | cut -w -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    HDT_SIZE=0
    NS_SIZE=$(du run/hdt-store | tail -n 1 | cut -w -f 1)

    echo "NATIVE,$SIZE,$TRIPLES_COUNT,$HDT_SIZE,$NS_SIZE" >> CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"


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

    mv "benchmark_result.xml" "../$RESULTS/nativestore/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done

fi

echo "kill NS ENDPOINT"
kill -KILL $NATIVE_EP_PID

echo "Benchmark done :)"
