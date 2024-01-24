<h1 align="center">
  <a href="https://github.com/the-qa-company/qEndpoint">
    <!-- Please provide path to your logo here -->
    <img src="docs/images/logo.svg" alt="Logo" width="100" height="100">
  </a>
</h1>

<div align="center">
  <b style = "font-size: 2em;">qEndpoint</b>
  <br />
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=bug&template=bug.yml">Report a Bug</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=enhancement&template=feature.yml">Request a Feature</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=support.yml">Ask a Question</a>
</div>

<div align="center">
<br />

[![Package build and deploy](https://github.com/the-qa-company/qEndpoint/actions/workflows/package-build.yml/badge.svg)](https://github.com/the-qa-company/qEndpoint/actions/workflows/package-build.yml) [![Tests](https://github.com/the-qa-company/qEndpoint/actions/workflows/test.yml/badge.svg)](https://github.com/the-qa-company/qEndpoint/actions/workflows/test.yml) 

**dev**

[![Tests](https://github.com/the-qa-company/qEndpoint/actions/workflows/test.yml/badge.svg?branch=dev)](https://github.com/the-qa-company/qEndpoint/actions/workflows/test.yml)

The QA Company over the social networks

<div id="badges">
  <a href="https://www.linkedin.com/company/qanswer/">
    <img src="https://cdn1.iconfinder.com/data/icons/logotypes/32/square-linkedin-512.png" width="25px" alt="LinkedIn Badge"/>
  </a>
  <a href="https://the-qa-company.com/">
    <img src="https://cdn3.iconfinder.com/data/icons/social-media-square-4/1024/square-10-512.png" width="25px" alt="The QA Company Web"/>
  </a>
  <a href="https://twitter.com/TheQACompany">
    <img src="https://cdn1.iconfinder.com/data/icons/logotypes/32/square-twitter-512.png" width="25px" alt="Twitter Badge"/>
  </a>
  <a href="mailto:info@the-qa-company.com">
    <img src="https://cdn4.iconfinder.com/data/icons/address-book-providers-in-colors/512/outlook-512.png" width="26px" alt="Email Badge"/>
  </a>
</div>

---

</div>


<details open="open">
<summary>Table of Contents</summary>

- [About](#about)
  - [Built With](#built-with)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Scoop](#scoop)
    - [Brew](#brew)
    - [Command Line Interface](#command-line-interface)
    - [Code](#code)
      - [Back-end](#back-end)
      - [Front-end](#front-end)
    - [Installers](#installers)
- [Usage](#usage)
  - [Docker Image](#docker-image)
    - [`qacompany/qendpoint`](#qacompanyqendpoint)
    - [`qacompany/qendpoint-wikidata`](#qacompanyqendpoint-wikidata)
    - [Useful tools](#useful-tools)
  - [Standalone](#standalone)
  - [As a dependency](#as-a-dependency)
- [Connecting with your Wikibase](#connecting-with-your-wikibase)
- [Roadmap](#roadmap)
- [Support](#support)
- [Project assistance](#project-assistance)
- [Contributing](#contributing)
- [Authors \& contributors](#authors--contributors)
- [Security](#security)
- [Publications](#publications)
- [License](#license)


</details>

---

## About

The qEndpoint is a highly scalable triple store with full-text and  [GeoSPARQL](https://www.ogc.org/standards/geosparql) support. It can be used as a standalone SPARQL endpoint, or as a dependency.
The qEndpoint is for example used in [Kohesio](https://kohesio.ec.europa.eu/) where each interaction with the UI corresponds to an underlying SPARQL query on the qEndpoint.
  
### Built With

- [Spring](https://spring.io/)
- [RDF-HDT](https://www.rdfhdt.org/)
- [RDF4J](https://rdf4j.org/)

---

## Getting Started

### Prerequisites

For the backend/benchmark

- Java 17
- Maven

For the frontend (not mandatory to run the backend)

- see specific [README](./qendpoint-frontend/README.md).

### Installation

#### Scoop

You can install qEndpoint using the [Scoop package manager](http://scoop.sh/).

You need to add the [`the-qa-company` bucket](https://github.com/the-qa-company/scoop-bucket), and then you will be able to install the `qendpoint` manifest, it can be done using these commands

```powershell
# Add the-qa-company bucket
scoop bucket add the-qa-company https://github.com/the-qa-company/scoop-bucket.git
# Install qEndpoint CLI
scoop install qendpoint
```

#### Brew

You can install qEndpoint using the [Brew package manager](http://brew.sh/).

You can install is using this command

```bash
brew install the-qa-company/tap/qendpoint
```

#### Command Line Interface

If you don't have access to Brew or Scoop, the qEndpoint command line interface is available in [the releases page](https://github.com/the-qa-company/qEndpoint/releases) under the file `qendpoint-cli.zip`. By extracting it, you can a bin directory that can be added to your path.

#### Code

##### Back-end 

- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the back-end directory `cd qendpoint-backend`
- Compile the project using this command: `mvn clean install -DskipTests`
- Run the project using `java -jar target/qendpoint-backend-1.2.3-exec.jar` (replace the version by the latest version)

 
  You can use the project as a dependency (replace the version by the latest version)

```
<dependency>
    <groupId>com.the_qa_company</groupId>
    <artifactId>qendpoint</artifactId>
    <version>1.2.3</version>
</dependency>
```

##### Front-end

- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the front-end directory `cd qendpoint-frontend`
- Install the packages using `npm install`
- Run the project using `npm start`

#### Installers

The endpoint installers for Linux, MacOS and Windows can be found [here](https://github.com/the-qa-company/qEndpoint/releases), **the installers do not contain the command line (cli), only the endpoint**.

---

## Usage

### Docker Image

You can use one of our preconfigured Docker images.

#### `qacompany/qendpoint`

**DockerHub**: [qacompany/qendpoint](https://hub.docker.com/r/qacompany/qendpoint)

This Docker 
  image contains the endpoint, you can upload your dataset and start using it.

You just have to run the image and it will prepare the environment by downloading the index and setting up the repository using the snippet below:

```bash
docker run -p 1234:1234 --name qendpoint qacompany/qendpoint
```

You can also specify the size of the memory allocated by setting the docker environnement value _MEM_SIZE_. By default this value is set to 6G. You should not set this value below 4G because you will certainly run out of memory with large dataset. For bigger dataset, a bigger value is also recommended for big dataset, as an example, [Wikidata-all](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) won't run without at least 10G.

```bash
docker run -p 1234:1234 --name qendpoint --env MEM_SIZE=6G qacompany/qendpoint
```

You can stop the container and rerun it at anytime maintaining the data inside (qendpoint is the name of the container) using the following commands:

```bash
docker stop qendpoint
docker start qendpoint
```

: **Note** this container may occupy a huge portion of the disk due to the size of the data index, so make sure to delete the container if you don't need it anymore by using the command below:

```bash
docker rm qendpoint
```

#### `qacompany/qendpoint-wikidata`

**DockerHub**: [qacompany/qendpoint-wikidata](https://hub.docker.com/r/qacompany/qendpoint-wikidata)

This Docker image contains the endpoint with a script to download an index containing the [Wikidata Truthy](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements from our servers, so you simply have to wait for the index download and start using it.

You just have to run the image and it will prepare the environment by downloading the index and setting up the repository using the code below:

```bash
docker run -p 1234:1234 --name qendpoint-wikidata qacompany/qendpoint-wikidata
```

You can also specify the size of the memory allocated by setting the docker environnement value _MEM_SIZE_. By default this value is set to 6G, a bigger value is also recommended for big dataset, as an example, [Wikidata-all](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) won't run without at least 10G.

```bash
docker run -p 1234:1234 --name qendpoint-wikidata --env MEM_SIZE=6G qacompany/qendpoint-wikidata
```

You can specify the dataset to download using the environnement value _HDT_BASE_, by default the value is `wikidata_truthy`, but the current available values are:

- `wikidata_truthy` - [Wikidata Truthy](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements (need at least `6G` of memory)
- `wikidata_all` - [Wikidata-all ](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements (need at least `10G` of memory)

```bash
docker run -p 1234:1234 --name qendpoint-wikidata --env MEM_SIZE=10G --env HDT_BASE=wikidata_all qacompany/qendpoint-wikidata
```

You can stop the container and rerun it at anytime maintaining the data inside (qendpoint is the name of the container) using the below code:

```bash
docker stop qendpoint-wikidata
docker start qendpoint-wikidata
```

**Note** this container may occupy a huge portion of the disk due to the size of the data index, so make sure to delete the container if you don't need it anymore using the command as shown below:

```bash
docker rm qendpoint-wikidata
```

#### Useful tools

You can access http://localhost:1234 where there is a GUI where you can write SPARQL queries and execute them, and there is the RESTful API available which you can use to run queries from any application over HTTP like so:

```bash
curl -H 'Accept: application/sparql-results+json' localhost:1234/api/endpoint/sparql --data-urlencode 'query=select * where{ ?s ?p ?o } limit 10'
```

**Note** first query will take some time in order to map the index to memory, later on it will be much faster!

Most of the result formats are available, you can use for example:

- JSON: `application/sparql-results+json`
- XML: `application/sparql-results+xml`
- Binary RDF: `application/x-binary-rdf-results-table`

### Standalone

You can run the endpoint with this command:

```bash
java -jar endpoint.jar &
```

you can find a template of [the application.properties file in the backend source](qendpoint-backend/src/main/resources/application.properties)

If you have the HDT file of your graph, you can put it before loading the endpoint in the hdt-store directory (by default `hdt-store/index_dev.hdt`)

If you don't have the HDT, you can upload the dataset to the endpoint by running the command while the endpoint is running:

```bash
curl "http://127.0.0.1:1234/api/endpoint/load" -F "file=@mydataset.nt"
```

where `mydataset.nt` is the RDF file to load, you can use all [the formats used by RDF4J](https://rdf4j.org/javadoc/latest/org/eclipse/rdf4j/rio/RDFFormat.html).

### As a dependency

You can create a SPARQL repository using this method, don't forget to init the repository

```java
// Create a SPARQL repository
SparqlRepository repository = CompiledSail.compiler().compileToSparqlRepository();
// Init the repository
repository.init();
```

You can execute SPARQL queries using the `executeTupleQuery`, `executeBooleanQuery`, `executeGraphQuery` or `execute`.

```java
// execute the a tuple query
try (ClosableResult<TupleQueryResult> execute = sparqlRepository.executeTupleQuery(
        // the sparql query
        "SELECT * WHERE { ?s ?p ?o }",
        // the timeout
        10
)) {
    // get the result, no need to close it, closing execute will close the result
    TupleQueryResult result = execute.getResult();

    // the tuples
    for (BindingSet set : result) {
        System.out.println("Subject:   " + set.getValue("s"));
        System.out.println("Predicate: " + set.getValue("p"));
        System.out.println("Object:    " + set.getValue("o"));
    }
}
```

Don't forget to shutdown the repository after usage

```java
// Shutdown the repository (better to release resources)
repository.shutDown();
```

You can get the RDF4J repository with the `getRepository()` method.

```java
// get the rdf4j repository (if required)
SailRepository rdf4jRepo = repository.getRepository();
```
---

## Connecting with your Wikibase

- run the qEndpoint locally
- `cd wikibase`
- move the file `prefixes.sparql` to your qEndpoint installation
- (re-)start your endpoint to use the prefixes
- run

  ```bash
  java -cp wikidata-query-tools-0.3.59-SNAPSHOT-jar-with-dependencies.jar org.wikidata.query.rdf.tool.Update \
          --sparqlUrl http://localhost:1234/api/endpoint/sparql \
          --wikibaseHost https://linkedopendata.eu/ \
          --wikibaseUrl https://linkedopendata.eu/ \
          --conceptUri https://linkedopendata.eu/ \
          --wikibaseScheme https \
          --entityNamespaces 120,122 \
          --start 2022-06-28T11:27:08Z
  ```

  you can adapt the parameters to your wikibase, in this case we are querying the [EU Knowledge Graph](https://linkedopendata.eu/), you may also change the start time.

---
## Roadmap

See the [open issues](https://github.com/the-qa-company/qEndpoint/issues) for a list of proposed features (and known issues).

- [Top Feature Requests](https://github.com/the-qa-company/qEndpoint/issues?q=label%3Aenhancement+is%3Aopen+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Top Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aissue+is%3Aopen+label%3Abug+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Newest Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aopen+is%3Aissue+label%3Abug)

---

## Support

Reach out to the maintainer at one of the following places:

- [GitHub issues](https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=04_SUPPORT_QUESTION.md&title=support%3A+)
- Contact options listed on [this GitHub profile](https://github.com/the-qa-company)
- [The QA Company website](https://the-qa-company.com/)

---

## Project assistance

If you want to say **thank you** or/and support active development of qEndpoint:

- Add a [GitHub Star](https://github.com/the-qa-company/qEndpoint) to the project ⭐
- [Tweet](https://twitter.com/TheQACompany) about the qEndpoint
- Write interesting articles about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog.

---

## Contributing

First of all, **thanks** for taking the time to contribute! Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make will benefit everybody else and are **greatly appreciated**.

Please read [our contribution guidelines](.github/CONTRIBUTING.md), and thank you for being involved!

---

## Authors & contributors

The original setup of this repository is by [The QA Company](https://github.com/the-qa-company).

For a full list of all authors and contributors, see [the contributors page](https://github.com/the-qa-company/qEndpoint/contributors).

---

## Security

qEndpoint follows good practices of security, but 100% security cannot be assured.
qEndpoint is provided **"as is"** without any **warranty**. Use at your own risk.

_For more information and to report security issues, please refer to our [security documentation](docs/SECURITY.md)._

---

## Publications

- Willerval Antoine, Dennis Diefenbach, and Pierre Maret. "Easily setting up a local Wikidata SPARQL endpoint using the qEndpoint." Workshop ISWC (2022). [PDF](https://www.researchgate.net/publication/364321138_Easily_setting_up_a_local_Wikidata_SPARQL_endpoint_using_the_qEndpoint)
- Willerval Antoine, Dennis Diefenbach, Angela Bonifati. "qEndpoint: A Wikidata SPARQL endpoint on commodity hardware" Demo at The Web Conference (2023)  [PDF](https://www.researchgate.net/publication/369693531_qEndpoint_A_Wikidata_SPARQL_endpoint_on_commodity_hardware)

---

## License

This project is licensed under the **GNU General Public License v3** with a notice.

See [LICENSE](LICENSE.md) for more information.

---

<div id="badges" align="right">
    <h4>
      Let's Connect
    </h4>
    <a href="https://www.linkedin.com/company/qanswer/">
      <img src="https://cdn1.iconfinder.com/data/icons/logotypes/32/square-linkedin-512.png" width="18px" alt="LinkedIn Badge"/>
    </a>
    <a href="https://the-qa-company.com/">
      <img src="https://cdn3.iconfinder.com/data/icons/social-media-square-4/1024/square-10-512.png" width="18px" alt="The QA Company Web"/>
    </a>
    <a href="https://twitter.com/TheQACompany">
      <img src="https://cdn1.iconfinder.com/data/icons/logotypes/32/square-twitter-512.png" width="18px" alt="Twitter Badge"/>
    </a>
    <a href="mailto:info@the-qa-company.com">
      <img src="https://cdn4.iconfinder.com/data/icons/address-book-providers-in-colors/512/outlook-512.png" width="19px" alt="Email Badge"/>
    </a>
</div>
