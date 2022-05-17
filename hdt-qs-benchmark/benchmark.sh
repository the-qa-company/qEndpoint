#!/usr/bin/env bash

# URL/PORT of the endpoint
PORT=1245
ENDPOINTURL="http://127.0.0.1:$PORT/api/endpoint"

# tests to run
# - use Endpoint store HDT LOAD method?
RUN_HS_LOADED=true
# - use Endpoint store HDT MAP method?
RUN_HS_MAPPED=true
# - use native store?
RUN_NS=true
# - use LMDB store?
RUN_LMDB=false

# test modes: "update", "bi", "explore"
RUN_MODES="explore update bi"
# params for the generator/testdriver
GENERATOR_PARAMS=
TESTDRIVER_PARAMS=
# BSBM products count (separate with spaces)
TESTS_NUMBERS="10000 50000 100000 200000"
# TESTS_NUMBERS="500000 1000000 2000000"
# TESTS_NUMBERS="10000"
# Timeout before killing the EP (in seconds)
TIMEOUT_SECOND=$((3600 * 24 * 7))
# Rebuild the endpoint at each start of the benchmark
REBUILD_ENDPOINT=false
# Endpoint jar file
ENDPOINT_JAR=endpoint.jar
# Max memory for the Endpoint
JAVA_MAX_MEM=32G
# Time to wait for the endpoint to start (in seconds)
ENDPOINT_WAIT=15

# Endpoint URL
#   Param in the UPDATE query GET method
UPDATE_PARAM=query
SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load

# output file for bsbm
OUTPUT=output
# run directory
RUN=run
# old run directory
RUN_OLD=run_old
# csv stats file
CSV_FILE=results.csv
# results directory
RESULT_DIRECTORY=results

# Usage:   runtest MODE READING RESULTS_XML RDF_MODEL RMODE
# Example: runtest "HYBRID" "MAP" "hybridstore/map" "model_hs_map" "bi"
function runtest {
    MODE=$1
    READING=$2
    RESULTS_XML=$3
    RDF_MODEL=$4
    RMODE=$5
    
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
        
        GENERATOR_PARAMS_IN=$GENERATOR_PARAMS
        TESTDRIVER_PARAMS_IN=$TESTDRIVER_PARAMS
        
        case "${RMODE}" in
            update)
                GENERATOR_PARAMS_IN="$GENERATOR_PARAMS_IN  -ud"
                DATASET_LOCATION=dataset_update.nt
            ;;
            *)
                DATASET_LOCATION=$OUTPUT_IN/dataset$SIZE.nt
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
        
        echo "Start sending $OUTPUT_IN/dataset$SIZE file with $TRIPLES_COUNT triples"
        
        # Send the dataset to the endpoint
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
        -uqp $UPDATE_PARAM \
        -udataset $DATASET_LOCATION \
        -idir "$OUTPUT_IN/data$SIZE" \
        -w $WARMUP_RUN \
        -runs $QUERY_MIX_RUNS \
        $TESTDRIVER_PARAMS_IN \
        "$SPARQL_URL"
        then
            1>&2 echo "Can't test dataset$SIZE"
            kill -KILL $HDT_EP_PID_MAP
            kill -SIGUSR1 $TIMEOUT_PID
            rm -rf "../$RUNOUT/$RESULTS_XML/$SIZE"
            mkdir -p "../$RUNOUT/$RESULTS_XML/$SIZE"
            mv "../$RUN" "../$RUNOUT/$RESULTS_XML/$SIZE"
            return
        fi
        
        # compute stats for the CSV
        RUN_SIZE=$(du -h ../$RUN | tail -n 1 | cut -f 1)
        HDT_SIZE=$(du -h ../$RUN/hdt-store | tail -n 1 | cut -f 1)
        NS_SIZE=$(du -h ../$RUN/native-store | tail -n 1 | cut -f 1)
        RESULTS_XML_FILE="$RESULTS_XML/benchmark_result_$SIZE.xml"
        
        echo "Completed test with runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"
        echo "$MODE,$READING,$RESULTS_XML_FILE,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
        
        echo "Write results into $RESULTS/$RESULTS_XML_FILE"
        mv "benchmark_result.xml" "../$RESULTS/$RESULTS_XML_FILE"
        
        echo "kill ENDPOINT and timeout"
        
        kill -SIGUSR1 $TIMEOUT_PID
        kill -KILL $HDT_EP_PID_MAP
        
        # Remove the dataset
        rm -rf "../$RUNOUT/$RESULTS_XML/$SIZE"
        mkdir -p "../$RUNOUT/$RESULTS_XML/$SIZE"
        mv "../$RUN" "../$RUNOUT/$RESULTS_XML/$SIZE"
        rm -rf "$OUTPUT_IN"
    done
    
}

for RUN_MODE in $RUN_MODES
do
    echo "run mode $RUN_MODE"
    # config per mode
    case "${RUN_MODE}" in
        update)
            echo "BSBM UPDATE MODE"
            # dir configs
            RUNOUT=$RUN_OLD/upt
            RESULTS=$RESULT_DIRECTORY/update
            
            # bsbm config
            USECASES=exploreAndUpdate
            WARMUP_RUN=50
            QUERY_MIX_RUNS=500
        ;;
        bi)
            echo "BSBM BI MODE"
            # dir configs
            RUNOUT=$RUN_OLD/bi
            RESULTS=$RESULT_DIRECTORY/bi
            
            # bsbm config
            USECASES=businessIntelligence
            WARMUP_RUN=25
            QUERY_MIX_RUNS=10
        ;;
        explore)
            echo "BSBM EXPLORE MODE"
            # dir configs
            RUNOUT=$RUN_OLD/exp
            RESULTS=$RESULT_DIRECTORY/explore
            
            # bsbm config
            USECASES=explore
            WARMUP_RUN=50
            QUERY_MIX_RUNS=500
        ;;
        *)
            echo "BAD BSBM MODE: $RUN_MODE. exit"
            exit -1
        ;;
    esac
    CSV=$RESULTS/$CSV_FILE

    trap "echo 'DELETE $RUN $OUTPUT' ; rm -rf $RUN $OUTPUT" EXIT INT SIGUSR1


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

    # Download the tool to generate the file
    if [ -d "bsbmtools" ]
    then
        echo "bsbmtools already installed, to delete it run 'rm -r bsbmtools'"
    else
        echo "Downloading BSBM..."
        curl https://phoenixnap.dl.sourceforge.net/project/bsbmtools/bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip --output bsmtools.zip
        unzip bsmtools.zip
        rm bsmtools.zip
        mv bsbmtools-0.2 bsbmtools
    fi

    # set into bsbm tool suite

    OUTPUT_IN=../$OUTPUT
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
        runtest "HYBRID" "MAP" "hybridstore/map" "model_hs_map" $RUN_MODE
    fi

    if $RUN_HS_LOADED
    then
        runtest "HYBRID" "LOAD" "hybridstore/load" "model_hs_load" $RUN_MODE
    fi

    if $RUN_NS
    then
        runtest "NATIVE" "DEFAULT" "nativestore" "model_ns" $RUN_MODE
        
    fi

    if $RUN_LMDB
    then
        runtest "LMDB" "DEFAULT" "lmdb" "model_lmdb" $RUN_MODE
    fi

    cd ..

done

echo "Benchmark done :)"
