#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=http://127.0.0.1:1236/api/endpoint

# test to run
RUN_HS_LOADED=false
RUN_HS_MAPPED=false
RUN_NS=true
RUN_LMDB=false
# test modes: "update", "bi", "explore"
RUN_MODE=explore
# params for the generator/testdriver
GENERATOR_PARAMS=
TESTDRIVER_PARAMS=
# BSBM products count (separate with spaces)
TESTS_NUMBERS="2000000"
# Max memory for the Endpoint
JAVA_MAX_MEM=32G
# Timeout before killing the EP
TIMEOUT_SECOND=$((3600 * 128))

# Endpoint URL
SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load

# Directories/Output file
OUTPUT=output
RUN=run
CSV=$RESULTS/results.csv


case "${RUN_MODE}" in
    update)
        echo "update MODE"
        RESULTS=results_update
        USECASES=exploreAndUpdate
        RUNOUT=run_out_upt
    ;;
    bi)
        echo "BI MODE"
        RESULTS=results_bi
        USECASES=businessIntelligence
        RUNOUT=run_bi
    ;;
    explore)
        echo "EXPLORE"
        RESULTS=results_explore
        USECASES=explore
        RUNOUT=run_out
    ;;
    *)
        echo "BAD MODE: $RUN_MODE"
        exit -1
    ;;
esac

trap "echo 'DELETE $RUN $OUTPUT' ; rm -rf $RUN $OUTPUT" EXIT INT SIGUSR1

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
    for SIZE in $TESTS_NUMBERS
    do
        echo "Generating $OUTPUT/dataset$SIZE..."
        # Remove previous dataset
        rm -rf "$OUTPUT_IN"
        mkdir -p "$OUTPUT_IN"
        
        cd ..
        
        rm -rf $RUN
        mkdir -p $RUN
        cp endpoint.jar $RUN
        
        cp application.properties "$RUN/application.properties"
        
        echo "repoModel=../models/$RDF_MODEL.ttl" >> "$RUN/application.properties"
        
        cd $RUN
        java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &
        HDT_EP_PID_MAP=$!
        echo "PID ENDPOINT:  $HDT_EP_PID_MAP"
        
        echo "Waiting for SPRING to load..."
        
        sleep 4
        
        cd ../bsbmtools
        
        ../timeoutkill.sh $HDT_EP_PID_MAP "$MODE-$READING" $TIMEOUT_SECOND &
        TIMEOUT_PID=$!
        echo "PID TIMEOUT: $TIMEOUT_PID"
        trap "echo 'killing EP EXIT';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID" EXIT
        trap "echo 'killing EP INT/SIGUSR1';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID; exit -1" INT SIGUSR1
        
        GENERATOR_PARAMS_IN=$GENERATOR_PARAMS
        TESTDRIVER_PARAMS_IN=$TESTDRIVER_PARAMS
        
        case "${RUN_MODE}" in
            update)
                GENERATOR_PARAMS_IN=$GENERATOR_PARAMS_IN  -ud
            ;;
            bi)
                TESTDRIVER_PARAMS_IN=$TESTDRIVER_PARAMS_IN –w 25 –runs 10
            ;;
            *)
            ;;
        esac
        
        # Generate the dataset
        TRIPLES_COUNT=$(./generate \
            -s nt \
            -pc $SIZE \
            -dir "$OUTPUT_IN/data$SIZE" \
            -fn "$OUTPUT_IN/dataset$SIZE" \
            $GENERATOR_PARAMS_IN \
        | tail -n 1 | cut -d " " -f 1)
        
        echo "Start sending files with $TRIPLES_COUNT triples"
        
        if curl "$LOAD_URL" \
        -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
        then
            echo "NT file Loaded"
        else
            echo "Can't load the NT file"
            kill -KILL $HDT_EP_PID_MAP
            kill -SIGUSR1 $TIMEOUT_PID
            rm -rf "$OUTPUT_IN"
            return
        fi
        
        echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, update: $UPDATE_TEST"
        
        # Test the dataset
        if ! ./testdriver \
        -ucf usecases/$USECASES/sparql.txt \
        -u "$UPDATE_URL" \
        -udataset "dataset_update.nt" \
        -idir "$OUTPUT_IN/data$SIZE" \
        $TESTDRIVER_PARAMS_IN \
        "$SPARQL_URL"
        then
            1>&2 echo "Can't test dataset$SIZE"
            kill -KILL $HDT_EP_PID_MAP
            kill -SIGUSR1 $TIMEOUT_PID
            rm -rf "$OUTPUT_IN"
            return
        fi
        
        RUN_SIZE=$(du ../$RUN | tail -n 1 | cut -f 1)
        HDT_SIZE=$(du ../$RUN/hdt-store | tail -n 1 | cut -f 1)
        NS_SIZE=$(du ../$RUN/native-store | tail -n 1 | cut -f 1)
        RESULTS_XML_FILE="$RESULTS_XML/benchmark_result_$SIZE.xml"
        
        echo "Completed test with runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"
        echo "$MODE,$READING,$RESULTS_XML_FILE,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
        
        mv "benchmark_result.xml" "../$RESULTS/$RESULTS_XML_FILE"
        
        echo "kill ENDPOINT and timeout"
        
        kill -SIGUSR1 $TIMEOUT_PID
        kill -KILL $HDT_EP_PID_MAP
        
        # Remove the dataset
        rm -rf "../$RUNOUT/$RESULTS_XML"
        mkdir -p "../$RUNOUT/$RESULTS_XML"
        mv "../$RUN" "../$RUNOUT/$RESULTS_XML"
        rm -rf "$OUTPUT_IN"
    done
    
}

#./build_endpoint.sh $RUN

echo "(Re)create result dir..."
mkdir -p "$RESULTS"

echo "Downloading BSBM..."
# Download the tool to generate the file
if [ -d "bsbmtools" ]
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

mkdir -p $RUNOUT

cd bsbmtools

if [ -f $CSV_IN ]
then
    mv $CSV_IN "$CSV_IN$(date '+%Y-%m-%d-%H:%M:%S').csv"
fi
echo "store,mode,file,uid,triples,runSize,hdtSize,nativeSize" > "$CSV_IN"

if $RUN_HS_MAPPED
then
    runtest "HYBRID" "MAP" "hybridstore/map" "model_hs_map"
fi

if $RUN_HS_LOADED
then
    runtest "HYBRID" "LOAD" "hybridstore/load" "model_hs_load"
    
fi

if $RUN_NS
then
    runtest "NATIVE" "DEFAULT" "nativestore" "model_ns"
    
fi

if $RUN_LMDB
then
    runtest "LMDB" "DEFAULT" "lmdb" "model_lmdb"
fi

echo "Benchmark done :)"
