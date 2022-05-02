#!/usr/bin/env bash

if ! (( $# == 1 ))
then
    1>&2 echo "$0 (run_dir)"
    exit -1
fi

mkdir -p $1

cd ../hdt-qs-backend

mvn clean install -DskipTests -U

cd ../hdt-qs-benchmark

cp ../hdt-qs-backend/target/hdtSparqlEndpoint-*-exec.jar $1/endpoint.jar