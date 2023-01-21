FILE=/app/qendpoint/hdt-store/index_dev.hdt
FILE2=$FILE.index.v1-1
CDN=https://qanswer-svc4.univ-st-etienne.fr
HDT_BASE=index_big

if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "starting..."
    echo "Downloading the HDT index $HDT_BASE..."
    mkdir -p qendpoint/hdt-store

    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE.tmp "$CDN/$HDT_BASE.hdt"
    mv $FILE.tmp $FILE

fi

if [ -f "$FILE2" ]; then
    echo "$FILE2 exists."
else
    echo "Downloading the HDT co-index $HDT_BASE..."
    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE2.tmp "$CDN/$HDT_BASE.hdt.index.v1-1"
    mv $FILE2.tmp $FILE2
fi