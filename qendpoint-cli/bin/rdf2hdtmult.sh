#!/bin/bash

source `dirname $0`/javaenv.sh

$JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH -XX:NewRatio=1 -XX:SurvivorRatio=9 com.the_qa_company.qendpoint.core.tools.RDF2HDTMult $*

exit $?
