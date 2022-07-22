FILE=/app/qendpoint/hdt-store/index_dev.hdt
FILE2=/app/qendpoint/hdt-store/index_big.hdt.index.v1-1

if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "starting..."
    echo "Downloading the HDT index..."
    mkdir -p qendpoint/hdt-store

    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt -o index_big.hdt.tmp
    mv index_big.hdt.tmp index_big.hdt

fi

if [ -f "$FILE2" ]; then
    echo "$FILE2 exists."
else
  wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O $FILE.index.v1-1 https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt.index.v1-1 -o index_big.hdt.index.v1-1.tmp
  mv index_big.hdt.index.v1-1.tmp index_big.hdt.index.v1-1
fi