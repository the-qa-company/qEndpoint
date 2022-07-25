FILE=/app/qendpoint/hdt-store/index_dev.hdt
FILE2=/app/qendpoint/hdt-store/index_big.hdt.index.v1-1

if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "starting..."
    echo "Downloading the HDT index..."
    mkdir -p qendpoint/hdt-store

    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE.tmp https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt 
    mv $FILE.tmp $FILE

fi

if [ -f "$FILE2" ]; then
    echo "$FILE2 exists."
else
  wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE2.tmp https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt.index.v1-1 
  mv $FILE2.tmp $FILE2
fi