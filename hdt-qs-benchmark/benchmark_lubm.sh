#!/usr/bin/env bash

# TODO: read explore, write load/merge

# URL/PORT of the endpoint
PORT=1241
ENDPOINTURL="http://127.0.0.1:$PORT/api/endpoint"

# tests to run
# - use Endpoint store HDT LOAD method?
RUN_HS_LOADED=true
# - use Endpoint store HDT MAP method?
RUN_HS_MAPPED=true

HDT_LOCATIONS="index_dev.hdt index_dev2.hdt"

# Rebuild the endpoint at each start of the benchmark
REBUILD_ENDPOINT=false
# Endpoint jar file
ENDPOINT_JAR=endpoint.jar
# Max memory for the Endpoint
JAVA_MAX_MEM=32G
# Time to wait for the endpoint to start (in seconds)
ENDPOINT_WAIT=15

# BSBM products count (separate with spaces)
TESTS_NUMBERS="10000"
# Timeout before killing the EP (in seconds)
TIMEOUT_SECOND=$((3600 * 24 * 7))

# Endpoint URL
#   Param in the UPDATE query GET method
UPDATE_PARAM=query
SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load

# run directory
RUN=run
# old run directory
RUN_OLD=run_old_lubm
# csv stats file
CSV_FILE=results.csv
# results directory
RESULT_DIRECTORY=results_lubm

RUNOUT=$RUN_OLD
RESULTS=$RESULT_DIRECTORY
CSV=$RESULTS/$CSV_FILE

# Usage:   runtest MODE READING RESULTS_XML RDF_MODEL
# Example: runtest "HYBRID" "MAP" "hybridstore/map" "model_hs_map"
function runtest {
    MODE=$1
    READING=$2
    RESULTS_XML=$3
    RDF_MODEL=$4
    
    echo ""
    echo "----------------------------------------"
    echo "------ RUN $MODE ($READING) BENCHMARK"
    echo "----------------------------------------"
    echo ""
    
    cd ..
    
    mkdir -p "$RESULTS/$RESULTS_XML"
    
    cd bsbmtools
    for HDT_LOCATION in $HDT_LOCATIONS
    do
        echo "Generating $HDT_LOCATION..."
        # Remove previous dataset
        
        cd ..
        
        rm -rf $RUN
        mkdir -p $RUN
        cp $ENDPOINT_JAR $RUN
        
        # config endpoint
        cp application.properties "$RUN/application.properties"
        # write port
        echo "server.port=$PORT" >> "$RUN/application.properties"
        # write model to use
        echo "repoModel=../models/$RDF_MODEL.ttl" >> "$RUN/application.properties"
        
        
        # Start ENDPOINT
        cd $RUN
        java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar $ENDPOINT_JAR &
        HDT_EP_PID_MAP=$!
        echo "PID ENDPOINT:  $HDT_EP_PID_MAP"
        
        echo "Waiting for SPRING to load..."
        
        sleep $ENDPOINT_WAIT
        
        cd ../bsbmtools
        
        # Start timeout for the EP
        
        ../timeoutkill.sh $HDT_EP_PID_MAP "$MODE-$READING" $TIMEOUT_SECOND &
        TIMEOUT_PID=$!
        echo "PID TIMEOUT: $TIMEOUT_PID"
        
        # Setup traps to kill everything
        trap "echo 'killing EP EXIT';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID" EXIT
        trap "echo 'killing EP INT/SIGUSR1';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID; exit -1" INT SIGUSR1
        
        
        echo "Start testing NT file '$HDT_LOCATION' size: $TRIPLES_COUNT, update: $UPDATE_TEST"
        
        # compute stats for the CSV
        RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
        HDT_SIZE=$(du ../$RUN/hdt-store | tail -n 1 | cut -f 1)
        NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)
        RESULTS_XML_FILE="$RESULTS_XML/result_$HDT_LOCATION.log"
        
        echo "Completed test with runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"
        echo "$MODE,$READING,$RESULTS_XML_FILE,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
        
        echo "Write results into $RESULTS/$RESULTS_XML_FILE"
        mv "benchmark_result.xml" "../$RESULTS/$RESULTS_XML_FILE"
        
        echo "kill ENDPOINT and timeout"
        
        kill -SIGUSR1 $TIMEOUT_PID
        kill -KILL $HDT_EP_PID_MAP
        
        # Remove the dataset
        rm -rf "../$RUN"
    done
    
}

if $REBUILD_ENDPOINT
then
    ./build_endpoint.sh .
elif [ ! -e "$ENDPOINT_JAR" ]
then
    echo "$ENDPOINT_JAR doesn't exists, building a new one"
    ./build_endpoint.sh .
fi

echo "(Re)create result dir..."
mkdir -p "$RESULTS"

# set into bsbm tool suite

CSV_IN=../$RESULTS/$CSV_FILE

mkdir -p $RUNOUT

cd bsbmtools

# Backup old CSV data

if [ -f $CSV_IN ]
then
    mv $CSV_IN "$CSV_IN$(date '+%Y-%m-%d-%H:%M:%S').csv"
fi

# Write CSV header

echo "store,mode,file,uid,triples,runSize,hdtSize,nativeSize" > "$CSV_IN"

if $RUN_HS_MAPPED
then
    runtest "HYBRID" "MAP" "hybridstore/map" "model_hs_map"
fi

if $RUN_HS_LOADED
then
    runtest "HYBRID" "LOAD" "hybridstore/load" "model_hs_load"
    
fi

echo "Benchmark done :)"
