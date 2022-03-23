# QEndpoint

- [SPARQL EndPoint/Backend](hdt-qs-backend/README.md)
- [SPARQL Frontend](hdt-qs-frontend/README.md)
- [Endpoint benchmark](hdt-qs-benchmark/README.md)

## How to run the wikibase updater

- Clone the QEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Compile the project using this command: `mvn clean install -DskipTests`
- Put the hdt index in the hdt-store directory (by default index_dev.hdt)
- Start the attached jar for the wikibase updater by running

```
java -cp target/wikidata-query-tools-*-SNAPSHOT-jar-with-dependencies.jar org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:1234/api/endpoint/sparql --wikibaseHost https://linkedopendata.eu/ --wikibaseUrl https://linkedopendata.eu/ --conceptUri https://linkedopendata.eu/ --wikibaseScheme https --entityNamespaces 120,122
```

## Where to find the things

- The jar of the updater is on compute /home/ha07131t/qa-data/
- AS well the hdt index is in /home/ha07131t/qa-data/admin/eu/hdt_index/
