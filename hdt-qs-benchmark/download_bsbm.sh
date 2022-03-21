#!/usr/bin/env bash

if [ ! -f "bsbmtools" ]
then
    echo "bsbmtools already installed, to delete it run 'rm -r bsbmtools'"
    exit 0
fi

curl https://phoenixnap.dl.sourceforge.net/project/bsbmtools/bsbmtools/bsbmtools-0.2/bsbmtools-v0.2.zip --output bsmtools.zip
unzip bsmtools.zip
rm bsmtools.zip
mv bsbmtools-0.2 bsbmtools
