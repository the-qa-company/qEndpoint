FILE=/home/app/data/hdt-store/index_big.hdt
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "starting..."
    echo "Downloading the HDT index..."
    mkdir -p qendpoint/hdt-store
    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O /app/qendpoint/hdt-store/index_dev.hdt https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt
    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O /app/qendpoint/hdt-store/index_dev.hdt.index.v1-1 https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt.index.v1-1
fi
