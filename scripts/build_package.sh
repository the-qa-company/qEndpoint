#!/usr/bin/env bash

if (( $# < 2 )); then
    1>&2 echo "$0 (package file) (endpoint jar)"
    exit -1
fi

PACKAGE_FILE=$1
ENDPOINT_JAR=$2

BASE=`dirname $0`

BUILD_DIR="build"
INPUT_DIR="$BUILD_DIR/input"
rm -rf "$BASE/$BUILD_DIR"
mkdir -p "$BASE/$INPUT_DIR"
PACKAGE_FILE_FINAL="build/jpackage_final.cfg"
LICENSE="../LICENSE.md"

echo "Read config file"
CONFIG_DATA=$(cat $PACKAGE_FILE)
echo "Copy endpoint"
EXEC_JAR_NAME="endpoint.jar"
cp $ENDPOINT_JAR "$BASE/$INPUT_DIR/$EXEC_JAR_NAME"

cd $BASE

cd ../hdt-qs-backend

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

echo "Version: '$VERSION'"

cd ../scripts

echo "Create jpackage config file"

LICENSE_BLD="$INPUT_DIR/LICENSE"

cp $LICENSE $LICENSE_BLD

echo "
$(cat jpackage.cfg)

--main-jar $EXEC_JAR_NAME
--app-version $VERSION

$CONFIG_DATA
" > "$PACKAGE_FILE_FINAL"

echo "JPackage creation $PACKAGE_FILE_FINAL"

jpackage "@$PACKAGE_FILE_FINAL"
