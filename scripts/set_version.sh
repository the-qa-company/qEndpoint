#!/usr/bin/env bash

if (( $# < 1 )); then
    1>&2 echo "$0 (version)"
    exit -1
fi

VERSION=$1

BASE=`dirname $0`

cd $BASE

cd ../hdt-qs-backend/

OLD_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
echo "old version: $OLD_VERSION"

cp pom.xml pom.xml_backupsv

echo "set new version..."

mvn versions:set versions:commit -DnewVersion="$VERSION" -q

NEW_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

echo "new version: $NEW_VERSION"

# Test if the mvn version was set
if [[ "${VERSION}" != "${NEW_VERSION}" ]]; then
    1>&2 echo "the new version isn't the same as the asked version, abort"
    mv pom.xml_backupsv pom.xml
    exit -1
fi


cd ../release/


touch RELEASE.md_old
mv RELEASE.md_old RELEASE.md_old_backupsv

# Write new lines

echo "## Version $OLD_VERSION 
" > RELEASE.md_old

cat RELEASE.md >> RELEASE.md_old

# Write old lines

cat RELEASE.md_old_backupsv >> RELEASE.md_old

mv RELEASE.md RELEASE.md_backupsv

rm -f RELEASE.md

echo "Open release file"

vim RELEASE.md

cd $BASE

if [ ! -f "../release/RELEASE.md" ]; then
    1>&2 echo "no release file created, abort"
    mv ../hdt-qs-backend/pom.xml_backupsv ../hdt-qs-backend/pom.xml
    mv ../release/RELEASE.md_backupsv ../release/RELEASE.md
    mv ../release/RELEASE.md_old_backupsv ../release/RELEASE.md_old
    exit -1
fi

echo "Remove backup files"

rm -f ../hdt-qs-backend/pom.xml_backupsv ../release/RELEASE.md_backupsv ../release/RELEASE.md_old_backupsv
