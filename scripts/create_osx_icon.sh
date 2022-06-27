#!/usr/bin/env bash

if (( $# < 2 )); then
    1>&2 echo "$0 (in png file) (out icns file)"
    exit -1
fi

IN_FILE=$1
OUT_FILE=$2

BASE=`dirname $0`

mkdir -p "$BASE/build/icons.iconset"

cp $IN_FILE "$BASE/build/icons.iconset/icon.png"

sips -z 512 512   "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_512x512.png"
sips -z 512 512   "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_256x256@2x.png"
sips -z 256 256   "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_256x256.png"
sips -z 256 256   "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_128x128@2x.png"
sips -z 128 128   "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_128x128.png"
sips -z 64 64     "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_32x32@2x.png"
sips -z 32 32     "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_32x32.png"
sips -z 32 32     "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_16x16@2x.png"
sips -z 16 16     "$BASE/build/icons.iconset/icon.png" --out "$BASE/build/icons.iconset/icon_16x16.png"
cp "$BASE/build/icons.iconset/icon.png" "$BASE/build/icons.iconset/icon_512x512@2x.png"

iconutil -c icns "$BASE/build/icons.iconset" --output $OUT_FILE
rm -R "$BASE/build/icons.iconset"
