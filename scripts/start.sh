#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/../

COMPILE=false 
CLIENT=false

POSITIONAL=()
while (( $# > 0 )); do
    case "${1}" in
        -recompile|--recompile)
        COMPILE=true 
        shift
        ;;
        -client|--client)
        CLIENT=true 
        shift
        ;;
        --help)
        echo "$0 [options]"
        echo "Options: --recompile --client --help"
        exit 0
        ;;
        *) # unknown flag/switch
        POSITIONAL+=("${1}")
        shift
        ;;
    esac
done

set -- "${POSITIONAL[@]}" # restore positional params

if $COMPILE; then
   mvn clean install -DskipTests
fi


echo "Fetching name/version"
version=$(scripts/get_version.sh "project.version")
name=$(scripts/get_version.sh "project.artifactId")

jarFile="hdt-qs-backend/target/$name-$version-exec.jar"


echo "Starting $name v$version '$jarFile'..."

if $Client; then
    java -Xmx6G -jar $jarFile --client
else
    java -Xmx6G -jar $jarFile
fi
