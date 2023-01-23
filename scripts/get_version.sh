#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/..

mvn help:evaluate -Dexpression=project.version -q -DforceStdout
