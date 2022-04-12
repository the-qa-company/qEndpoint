#!/usr/bin/env bash

# URL of the endpoint
ENDPOINTURL=http://127.0.0.1:1236/api/endpoint

JAVA_MAX_MEM=32G
RUN_HS_LOADED=true
RUN_HS_MAPPED=true
RUN_NS=true
RUN_LMDB=true
UPDATE_TEST=true
GENERATOR_PARAMS=
TESTDRIVER_PARAMS=
# TESTS_NUMBERS="500000 1000000 2000000 5000000 10000000"
TESTS_NUMBERS="1000 2000 3000"
TIMEOUT_SECOND=$((3600 * 24))
SPARQL_URL=$ENDPOINTURL/sparql
UPDATE_URL=$ENDPOINTURL/update
LOAD_URL=$ENDPOINTURL/load
OUTPUT=output_lubm
RUN=run_lubm
RESULTS=results_lubm
CSV=$RESULTS/results.csv

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
    
    cd ../..
    
    mkdir -p "$RESULTS/$RESULTS_XML"
    
    rm -rf $RUN
    mkdir -p $RUN
    cp endpoint.jar $RUN
    
    cp application.properties "$RUN/application.properties"
    
    echo "repoModel=../models/$RDF_MODEL.ttl" >> "$RUN/application.properties"
    
    cd lubmtools/gen
    for SIZE in $TESTS_NUMBERS
    do
        echo "Generating $OUTPUT/dataset$SIZE..."
        # Remove previous dataset
        rm -rf "$OUTPUT_IN"
        mkdir -p "$OUTPUT_IN"
        
        cd ../../$RUN
        java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar endpoint.jar &
        HDT_EP_PID_MAP=$!
        
        echo "Waiting for SPRING to load..."
        
        sleep 4
        
        cd ../lubmtools/gen
        
        ../../timeoutkill.sh $HDT_EP_PID_MAP "$MODE-$READING" $TIMEOUT_SECOND &
        TIMEOUT_PID=$!
        trap "echo 'killing EP';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID" EXIT
        trap "echo 'killing EP';kill -KILL $HDT_EP_PID_MAP; kill -SIGUSR1 $TIMEOUT_PID; exit -1" INT SIGUSR1
        
        # Generate the dataset
        TRIPLES_COUNT=$(./generate \
            -s nt \
            -pc $SIZE \
            -dir "$OUTPUT_IN/data$SIZE" \
            -fn "$OUTPUT_IN/dataset$SIZE" \
            $GENERATOR_PARAMS \
        | tail -n 1 | cut -d " " -f 1)
        
        if ! $UPDATE_TEST
        then
            if curl "$LOAD_URL" \
            -F "file=@$OUTPUT_IN/dataset$SIZE.nt"
            then
                echo "NT file Loaded"
            else
                1>&2 echo "Can't load the NT file"
                kill -KILL $HDT_EP_PID_MAP
                kill -SIGUSR1 $TIMEOUT_PID
                rm -rf "$OUTPUT_IN"
                return
            fi
        fi
        
        echo "Start testing NT file '$SIZE' size: $TRIPLES_COUNT, update: $UPDATE_TEST"
        
        cd ../test
        
        # Test the dataset
        if ! ./testdriver \
        -ucf usecases/explore/sparql.txt \
        -u "$UPDATE_URL" \
        -udataset "$OUTPUT_IN/dataset$SIZE.nt" \
        -idir "$OUTPUT_IN/data$SIZE" \
        $TESTDRIVER_PARAMS \
        "$SPARQL_URL"
        then
            1>&2 echo "Can't test dataset$SIZE"
            kill -KILL $HDT_EP_PID_MAP
            kill -SIGUSR1 $TIMEOUT_PID
            rm -rf "$OUTPUT_IN"
            return
        fi
        
        RUN_SIZE=$(du ../../$RUN | tail -n 1 | cut -f 1)
        HDT_SIZE=$(du ../../$RUN/hdt-store | tail -n 1 | cut -f 1)
        NS_SIZE=$(du ../../$RUN/native-store | tail -n 1 | cut -f 1)
        RESULTS_XML_FILE="$RESULTS_XML/benchmark_result_$SIZE.xml"
        
        echo "Completed test with runSize: $RUN_SIZE hdtSize: $HDT_SIZE, nativeStoreSize: $NS_SIZE"
        echo "$MODE,$READING,$RESULTS_XML_FILE,$SIZE,$TRIPLES_COUNT,$RUN_SIZE,$HDT_SIZE,$NS_SIZE" >> $CSV_IN
        
        mv "benchmark_result.xml" "../$RESULTS/$RESULTS_XML_FILE"
        
        echo "kill ENDPOINT and timeout"
        
        kill -SIGUSR1 $TIMEOUT_PID
        kill -KILL $HDT_EP_PID_MAP
        
        # Remove the dataset
        rm -rf "$OUTPUT_IN"
    done
    
}

#./build_endpoint.sh $RUN

echo "(Re)create result dir..."
mkdir -p "$RESULTS"

echo "Downloading LUBM..."
# Download the tool to generate the file
if [ -d "lubmtools" ]
then
    echo "lubmtools already installed, to delete it run 'rm -r lubmtools'"
else
    mkdir -p lubmtools/gen
    mkdir -p lubmtools/test
    echo "LUBM - Downloading zips..."
    curl http://swat.cse.lehigh.edu/projects/lubm/GeneratorLinuxFix.zip --output lubmtools/generator.zip
    curl http://swat.cse.lehigh.edu/projects/lubm/uba1.7.zip --output lubmtools/gen.zip
    curl http://swat.cse.lehigh.edu/projects/lubm/ubt1.1.zip --output lubmtools/test.zip
    echo "LUBM - Extracting..."
    unzip lubmtools/gen.zip -d lubmtools/gen
    unzip lubmtools/test.zip -d lubmtools/test
    
    echo "LUBM - Patch paths..."
    unzip lubmtools/generator.zip -d lubmtools
    mv ./lubmtools/Generator.java ./lubmtools/gen/src/edu/lehigh/swat/bench/uba/Generator.java
    cd lubmtools/gen
    echo "LUBM - Patch paths - Clear"
    rm -rf classes
    echo "LUBM - Patch paths - Compile"
    javac ./src/edu/lehigh/swat/bench/uba/* -d ./classes/
    cd ../..
    echo "LUBM - Cleaning..."
    rm lubmtools/gen.zip lubmtools/test.zip lubmtools/generator.zip
fi

# set into lubmtools tool suite

OUTPUT_IN=../../$OUTPUT
CSV_IN=../../$RESULTS/results.csv

cd lubmtools/gen

if [ -f $CSV_IN ]
then
    mv $CSV_IN "$CSV_IN$(date '+%Y-%m-%d-%H:%M:%S').csv"
fi
echo "store,mode,file,uid,triples,runSize,hdtSize,nativeSize" > "$CSV_IN"

if $RUN_HS_LOADED
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
