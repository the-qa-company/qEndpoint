FILE=/home/app/data/hdt-store/index.hdt
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else 
    echo "starting..."
    echo "Downloading the HDT index..." && wget --progress=bar:force:noscroll -O /home/app/data/hdt-store/index.hdt https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt \
        && wget --progress=bar:force:noscroll -O /home/app/data/hdt-store/index.hdt.index.v1-1 https://qanswer-svc4.univ-st-etienne.fr/index_big.hdt.index.v1-1
fi
echo "Starting HDT Sparql Service..."
java -Xmx6G -Dspring.config.location=file:///home/app/application-prod.properties -jar /usr/local/lib/hdtSparqlEndpoint-*-SNAPSHOT.jar
