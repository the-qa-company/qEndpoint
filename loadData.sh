FILE=/home/app/data/hdt-store/index.hdt
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else 
    echo "Downloading the HDT index..." && wget --progress=bar:force:noscroll -O /home/app/data/hdt-store/index.hdt https://data.linkedopendata.eu/index_big.hdt \
        && wget --progress=bar:force:noscroll -O /home/app/data/hdt-store/index.hdt.index.v1-1 https://data.linkedopendata.eu/index_big.hdt.index.v1-1 
fi
echo "Starting HDT Sparql Service..."
java -Dspring.config.location=file:///home/app/application-prod.properties -jar /usr/local/lib/hdtSparqlEndpoint-*-SNAPSHOT.jar
