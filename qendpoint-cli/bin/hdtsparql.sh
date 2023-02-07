#!/bin/bash

source `dirname $0`/javaenv.sh

if [[ $1 =~ ^- ]]; then
	export option=$1
	shift
fi

export hdtFile=$1
shift

$JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.the_qa_company.qendpoint.corejena.cmd.HDTSparql $option "$hdtFile" "$*"

exit $?
