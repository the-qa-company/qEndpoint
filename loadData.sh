FILE=/home/app/data/hdt-store/index_big.hdt
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "starting..."
    echo "Downloading the HDT index..."
    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O /home/app/data/hdt-store/index_big.hdt https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt
    wget --progress=bar:force:noscroll -c --retry-connrefused --tries 0 --timeout 10 -O /home/app/data/hdt-store/index_big.hdt.index.v1-1 https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt.index.v1-1
fi
echo "Starting HDT Sparql Service..."
java -Xmx"$1" -Dspring.config.location=file:///home/app/application-prod.properties -jar /usr/local/lib/hdtSparqlEndpoint-*-SNAPSHOT.jar
