#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=http://127.0.0.1:1236/api/endpoint

SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load
OUTPUT=output
RUN=run
RESULTS=results
JAVA_MAX_MEM=32G
RUN_HS_LOADED=true
RUN_HS_MAPPED=true
RUN_NS=true
RUN_LMDB=true
CSV=$RESULTS/results.csv

trap "echo 'EXITING...'; exit -1" INT
trap "echo 'DELETE $RUN $OUTPUT' ; rm -rf $RUN $OUTPUT" EXIT

#./build_endpoint.sh $RUN

echo "(Re)create result dir..."
mkdir -p "$RESULTS"

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

if [ -f "results.csv" ]
then
    mv $CSV_IN "$CSV_IN$(date '+%Y-%m-%d-%H:%M:%S').csv"
fi
touch $CSV_IN

if $RUN_HS_LOADED
then

echo ""
echo "----------------------------------------"
echo "--- RUN HYBRIDSTORE MAPPED BENCHMARK ---"
echo "----------------------------------------"
echo ""

cd ..

mkdir -p "$RESULTS/hybridstore/map"

rm -rf $RUN
mkdir -p $RUN
cp endpoint.jar $RUN

cp application.properties "$RUN/application.properties"

echo "repoModel=../models/model_hs_map.ttl" >> "$RUN/application.properties"

cd $RUN
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &

HDT_EP_PID_MAP=$!
trap "echo 'killing HDT EP';kill -KILL $HDT_EP_PID_MAP" EXIT INT

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
    | tail -n 1 | cut -d " " -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
    HDT_SIZE=$(du ../$RUN/hdt-store | tail -n 1 | cut -f 1)
    NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)

    echo "HYBRID,MAP,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"

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

    mv "benchmark_result.xml" "../$RESULTS/hybridstore/map/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done

echo "kill HDT ENDPOINT"

kill -KILL $HDT_EP_PID_MAP

fi
cd bsbmtools

if $RUN_HS_LOADED
then

echo ""
echo "----------------------------------------"
echo "--- RUN HYBRIDSTORE LOADED BENCHMARK ---"
echo "----------------------------------------"
echo ""

cd ..

mkdir -p "$RESULTS/hybridstore/map"

rm -rf $RUN
mkdir -p $RUN
cp endpoint.jar $RUN

cp application.properties "$RUN/application.properties"

echo "repoModel=../models/model_hs_load.ttl" >> "$RUN/application.properties"

cd $RUN
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &

HDT_EP_PID_LOAD=$!
trap "echo 'killing HDT EP';kill -KILL $HDT_EP_PID_LOAD" EXIT INT

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
    | tail -n 1 | cut -d " " -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
    HDT_SIZE=$(du ../$RUN/hdt-store | tail -n 1 | cut -f 1)
    NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)

    echo "HYBRID,LOAD,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"

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

    mv "benchmark_result.xml" "../$RESULTS/hybridstore/load/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done

echo "kill HDT ENDPOINT"

kill -KILL $HDT_EP_PID_LOAD

fi

if $RUN_NS
then

echo ""
echo "----------------------------------------"
echo "------ RUN NATIVESTORE BENCHMARK -------"
echo "----------------------------------------"
echo ""

cd ..

mkdir -p "$RESULTS/nativestore"

rm -rf $RUN
mkdir -p $RUN
cp endpoint.jar $RUN

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
    | tail -n 1 | cut -d " " -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
    HDT_SIZE=0
    NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)

    echo "NATIVE,DEFAULT,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"


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


echo "kill NS ENDPOINT"
kill -KILL $NATIVE_EP_PID

fi

if $RUN_LMDB
then

echo ""
echo "----------------------------------------"
echo "------    RUN LMDB BENCHMARK     -------"
echo "----------------------------------------"
echo ""

cd ..

mkdir -p "$RESULTS/lmdb"

rm -rf $RUN
mkdir -p $RUN
cp endpoint.jar $RUN

cp application.properties "$RUN/application.properties"

echo "repoModel=../models/model_lmdb.ttl" >> "$RUN/application.properties"

cd $RUN
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &

LMDB_EP_PID=$!
trap "echo 'killing LMDB EP';kill -KILL $LMDB_EP_PID" EXIT INT

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
    | tail -n 1 | cut -d " " -f 1)

    if curl "$LOAD_URL" \
                 -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
    then
        echo "NT file Loaded"
    else
        1>&2 echo "Can't load the NT file"
        exit -1
    fi

    RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
    HDT_SIZE=0
    NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)

    echo "LMDB,DEFAULT,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
    echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"


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

    mv "benchmark_result.xml" "../$RESULTS/lmdb/benchmark_result_$SIZE.xml"

    # Remove the dataset
    rm -rf "$OUTPUT_IN"
done


echo "kill LMDB ENDPOINT"
kill -KILL $LMDB_EP_PID

fi

echo "Benchmark done :)"
