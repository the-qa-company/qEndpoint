#!/bin/bash

source `dirname $0`/javaenv.sh

#export MAVEN_OPTS="-Xmx6g"
#mvn exec:java -Dexec.mainClass="com.the_qa_company.qendpoint.core.tools.HDTCat" -Dexec.args="$*"

$JAVA $JAVA_OPTIONS -cp $CP:$CLASSPATH com.the_qa_company.qendpoint.core.tools.HDTCat $*

exit $?
