#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/../hdt-qs-backend

EXPRESSION="project.version"

if (( $# > 0 )); then
    EXPRESSION=$1
fi

mvn help:evaluate "-Dexpression=$EXPRESSION" -q -DforceStdout
