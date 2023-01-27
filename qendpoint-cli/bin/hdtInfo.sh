#!/bin/bash

source `dirname $0`/javaenv.sh

$JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.the_qa_company.qendpoint.core.tools.HDTInfo $*

exit $?
