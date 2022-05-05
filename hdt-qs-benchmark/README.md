[...](../README.md)

# Sparql Endpoint Benchmark

- [Sparql Endpoint Benchmark](#sparql-endpoint-benchmark)
  - [Config for the BSBM version of the benchmark](#config-for-the-bsbm-version-of-the-benchmark)
  - [Run BSBM benchmark](#run-bsbm-benchmark)
  - [Exctract state with output file](#exctract-state-with-output-file)
  - [Dataset size in function of the BSBM product count](#dataset-size-in-function-of-the-bsbm-product-count)

[link to the benchmark tools](https://sourceforge.net/projects/bsbmtools/), should be downloaded and extract inside the directory bsbmtools

## Config for the BSBM version of the benchmark

Config them in the benchmark.sh file

- `PORT` - URL/PORT of the endpoint
- `ENDPOINTURL` - Endpoint Url
- `RUN_HS_LOADED` - run the benchmark with the endpoint using load method
- `RUN_HS_MAPPED` - run the benchmark with the endpoint using map method
- `RUN_NS` - run the benchmark with rdf4j native store
- `RUN_LMDB` - run the benchmark with rdf4j LMDB store
- `RUN_MODE` - test mode, can be: "update", "bi", "explore"
- `TESTS_NUMBERS` - BSBM product count, separate with spaces to run multiple tests
- `TIMEOUT_SECOND` - Timeout before abording the process
- `REBUILD_ENDPOINT` - rebuild the endpoint before launching the benchmark
- `JAVA_MAX_MEM` - Allocated memory for the endpoint
- `ENDPOINT_WAIT` - Time for the sparql endpoint to start, increase it for slow config
- `OUTPUT` - BSBM datasets output directory
- `RUN` - endpoint run directory
- `RUN_OLD` - endpoint run directory save
- `CSV_FILE` - CSV file where storing the disk information
- `RESULT_DIRECTORY` - directory to save results

## Run BSBM benchmark

Run the benchmark with the command

```bash
./benchmark.sh
# or to redirect:
./benchmark.sh > output.out 2> output.err &
```

## Exctract state with output file

Get the state of the benchmark, will show information about the benchmark, the step and the process running

```bash
./show_exception.sh
```

## Dataset size in function of the BSBM product count

- `10000` = `3,5M`
- `50000` = `17M`
- `100000` = `34M`
- `200000` = `69M`
- `500000` = `173M`
- `1000000` = `346M`
- `2000000` = `692,62M`
- `5000000` = `1.7B`
