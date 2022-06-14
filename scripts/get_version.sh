#!/usr/bin/env bash

BASE=`dirname $0`

cd $BASE/../hdt-qs-backend

mvn help:evaluate -Dexpression=project.version -q -DforceStdout
