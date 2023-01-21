#!/usr/bin/env bash
INDEX_HDT=/app/qendpoint/hdt-store/index_dev.hdt
INDEX_HDT_COINDEX=$INDEX_HDT.index.v1-1
CDN=https://qanswer-svc4.univ-st-etienne.fr

# Read args
if (( $# == 0 )); then
    HDT_BASE=index_big
else
    HDT_BASE=$1
fi

HDT="$CDN/$HDT_BASE.hdt"

echo "Search for $HDT_BASE index at $HDT"

if [ -f "$INDEX_HDT" ]; then
    echo "$INDEX_HDT exists."
else
    echo "starting..."
    echo "Downloading the HDT index $HDT_BASE into $INDEX_HDT..."
    mkdir -p qendpoint/hdt-store


    if [ -f "$INDEX_HDT.tmp" ]; then
        rm "$INDEX_HDT.tmp"
    fi

    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $INDEX_HDT.tmp $HDT
    mv $INDEX_HDT.tmp $INDEX_HDT
fi

if [ -f "$INDEX_HDT_COINDEX" ]; then
    echo "$INDEX_HDT_COINDEX exists."
else
    echo "Downloading the HDT co-index $HDT_BASE into $INDEX_HDT_COINDEX..."

    if [ -f "$INDEX_HDT_COINDEX.tmp" ]; then
        rm "$INDEX_HDT_COINDEX.tmp"
    fi

    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $INDEX_HDT_COINDEX.tmp "$HDT.index.v1-1"
    mv $INDEX_HDT_COINDEX.tmp $INDEX_HDT_COINDEX
fi