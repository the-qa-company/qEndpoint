#!/usr/bin/env bash

BASE=`dirname $0`

cd "$BASE/../release"

if ! (cat PREFIX.md RELEASE.md SUFFIX.md > RELEASE_OUTPUT.md); then
    1>&2 echo "can't create RELEASE_OUTPUT.md file"
    exit -1
fi

echo "RELEASE_OUTPUT.md file created"