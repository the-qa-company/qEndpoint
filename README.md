<h1 align="center">
  <a href="https://github.com/the-qa-company/qEndpoint">
    <!-- Please provide path to your logo here -->
    <img src="docs/images/logo.svg" alt="Logo" width="100" height="100">
  </a>
</h1>

<div align="center">
  qEndpoint
  <br />
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=bug&template=bug.yml">Report a Bug</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=enhancement&template=feature.yml">Request a Feature</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=support.yml">Ask a Question</a>
</div>

<div align="center">
<br />

[![Pull Requests welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg?style=flat-square)](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)

</div>

<details open="open">
<summary>Table of Contents</summary>

- [About](#about)
  - [Built With](#built-with)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Standalone](#standalone)
  - [As a dependency](#as-a-dependency)
- [Roadmap](#roadmap)
- [Support](#support)
- [Project assistance](#project-assistance)
- [Contributing](#contributing)
- [Authors & contributors](#authors--contributors)
- [Security](#security)
- [License](#license)

</details>

---

## About

The qEndpoint is a highly scalable triple store with full-text and GeoSPARQL support. It can be used as a standalone SPARQL endpoint, or as a dependency.

### Built With

- [Spring](https://spring.io/)
- [RDF-HDT](https://www.rdfhdt.org/)
- [RDF4J](https://rdf4j.org/)

## Getting Started

### Prerequisites

For the backend/benchmark

- Java 11
- Maven

For the frontend (not mandatory to run the backend)

- see specific [README](./hdt-qs-frontend/README.md)

### Installation

#### Installers

Installers for Linux, MacOS and Windows can be found [here](https://github.com/the-qa-company/qEndpoint/releases)

#### Code

##### Back-end
- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the back-end directory `cd hdt-qs-backend`
- Compile the project using this command: `mvn clean install -DskipTests`
- Run the project using `java -jar target/hdtSparqlEndpoint-1.2.3-exec.jar` (replace the version by the latest version)

You can use the project as a dependency (replace the version by the latest version)

```
<dependency>
    <groupId>com.the_qa_company</groupId>
    <artifactId>hdtSparqlEndpoint</artifactId>
    <version>1.2.3</version>
</dependency>
```

##### Front-end
- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the front-end directory `cd hdt-qs-frontend`
- Install the packages using `npm install`
- Run the project using `npm start`


## Usage

### Standalone

You can run the endpoint with this command

```bash
java -jar endpoint.jar &
```

you can find a template of [the application.properties file in the backend source](hdt-qs-backend/src/main/resources/application.properties)

If you have the HDT file of your graph, you can put it before loading the endpoint in the hdt-store directory (by default `hdt-store/index_dev.hdt`)

If you don't have the HDT, you can upload the dataset to the endpoint by running the command while the endpoint is running

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

You can get the RDF4J with this method

```java
// get the rdf4j repository (if required)
SailRepository rdf4jRepo = repository.getRepository();
```

## Connecting with your Wikibase

- run the qEndpoint locally
- `cd wikibase`
- run ` 
java -cp wikidata-query-tools-0.3.59-SNAPSHOT-jar-with-dependencies.jar org.wikidata.query.rdf.tool.Update --sparqlUrl http://localhost:1234/api/endpoint/sparql --wikibaseHost https://linkedopendata.eu/ --wikibaseUrl https://linkedopendata.eu/ --conceptUri https://linkedopendata.eu/ --wikibaseScheme https --entityNamespaces 120,122 --start 2022-06-28T11:27:08Z` (adapt the parameters to your wikibase, in this case we are querying the "Eu Knowledge Graph")

## Roadmap

See the [open issues](https://github.com/the-qa-company/qEndpoint/issues) for a list of proposed features (and known issues).

- [Top Feature Requests](https://github.com/the-qa-company/qEndpoint/issues?q=label%3Aenhancement+is%3Aopen+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Top Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aissue+is%3Aopen+label%3Abug+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Newest Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aopen+is%3Aissue+label%3Abug)

## Support

Reach out to the maintainer at one of the following places:

- [GitHub issues](https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=04_SUPPORT_QUESTION.md&title=support%3A+)
- Contact options listed on [this GitHub profile](https://github.com/the-qa-company)
- [The QA Company website](https://the-qa-company.com/)

## Project assistance

If you want to say **thank you** or/and support active development of qEndpoint:

- Add a [GitHub Star](https://github.com/the-qa-company/qEndpoint) to the project.
- Tweet about the qEndpoint.
- Write interesting articles about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog.

Together, we can make qEndpoint **better**!

## Contributing

First off, thanks for taking the time to contribute! Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make will benefit everybody else and are **greatly appreciated**.

Please read [our contribution guidelines](docs/CONTRIBUTING.md), and thank you for being involved!

## Authors & contributors

The original setup of this repository is by [The QA Company](https://github.com/the-qa-company).

For a full list of all authors and contributors, see [the contributors page](https://github.com/the-qa-company/qEndpoint/contributors).

## Security

qEndpoint follows good practices of security, but 100% security cannot be assured.
qEndpoint is provided **"as is"** without any **warranty**. Use at your own risk.

_For more information and to report security issues, please refer to our [security documentation](docs/SECURITY.md)._

## License

This project is licensed under the **GNU General Public License v3**.

See [LICENSE](LICENSE.md) for more information.
