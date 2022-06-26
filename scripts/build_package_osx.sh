#!/usr/bin/env bash

if (( $# < 1 )); then
    1>&2 echo "$0 (endpoint jar)"
    exit -1
fi

ENDPOINT_JAR=$1

BASE=`dirname $0`

cp $ENDPOINT_JAR "$BASE/endpoint.jar"

cd $BASE

echo $MACOS_CERTIFICATE | base64 --decode > certificate.p12

echo "Create keychain 'build_keychain'"
security create-keychain -p "$MACOS_KEYCHAIN_PWD" build_keychain
echo "Default keychain 'build_keychain'"
security default-keychain -s build_keychain
echo "Unlock keychain 'build_keychain'"
security unlock-keychain -p "$MACOS_KEYCHAIN_PWD" build_keychain
echo "Import certificate"
security import certificate.p12 -k build_keychain -P $MACOS_CERTIFICATE_PWD -T /usr/bin/codesign -T /usr/bin/xcrun
echo "Set partition list"
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$MACOS_KEYCHAIN_PWD" build_keychain

cat jpackage_osx.cfg > tmp_jpackage_osx.cfg
echo "
--mac-sign
--mac-signing-key-user-name \"$MACOS_DEV_ID\"
" >> tmp_jpackage_osx.cfg

./build_package.sh tmp_jpackage_osx.cfg endpoint.jar

echo "signing dmg file"


mv "build/distributions/$(ls build/distributions)" "endpoint.dmg"

gon -log-level=debug -log-json ./gon.json

#codesign --timestamp -s "$MACOS_DEV_ID" "endpoint.dmg"

#xcrun notarytool store-credentials build_keychain
#xcrun notarytool submit "endpoint.dmg" --keychain-profile build_keychain --wait
#xcrun stapler staple "endpoint.dmg"
#spctl -a -t open --context context:primary-signature -vv "endpoint.dmg"

#echo "signed"

mv endpoint.dmg ..

rm endpoint.jar tmp_jpackage_osx.cfg


