<h1 align="center">
  <a href="https://github.com/the-qa-company/qEndpoint">
    <!-- Please provide path to your logo here -->
    <img src="docs/images/logo.svg" alt="Logo" width="100" height="100">
  </a>
</h1>

<div align="center">
  QEndpoint
  <br />
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=bug&template=bug.md&title=bug%3A+">Report a Bug</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=enhancement&template=feature.md&title=feat%3A+">Request a Feature</a>
  ·
  <a href="https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=support.md&title=support%3A+">Ask a Question</a>
</div>

<div align="center">
<br />

[![Project license](https://img.shields.io/github/license/the-qa-company/qEndpoint.svg?style=flat-square)](LICENSE)

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

QEndpoint is a highly scalable triple store with full-text and GeoSPARQL support. It can be used as a standalone SPARQL endpoint, or as a dependency.

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

- Node 14

### Installation

- Clone the QEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- move to the back-end directory `cd hdt-qs-backend`
- Compile the project using this command: `mvn clean install -DskipTests`

You can use the project as a dependency (replace the version by the latest version)

```
<dependency>
    <groupId>com.the_qa_company</groupId>
    <artifactId>hdtSparqlEndpoint</artifactId>
    <version>1.2.3</version>
</dependency>
```

Or you can get the executable jar of the endpoint `target/hdtSparqlEndpoint-VERSION-exec.jar`.

## Usage

### Standalone

You can run the endpoint with this command

```bash
java -Xmx"$JAVA_MAX_MEM" "-Dspring.config.location=application.properties" -jar ENDPOINT_JAR &
```

you can find a template of [the application.properties file in the backend source](hdt-qs-backend/src/main/resources/application-prod.properties)

If you have the HDT file of your graph, you can put it before loading the endpoint in the hdt-store directory (by default `hdt-store/index_dev.hdt`)

If you don't have the HDT, you can upload the dataset to the endpoint by running the command while the endpoint is running

```bash
curl "http://127.0.0.1:1234/api/endpoint/load" -F "file=@mydataset.nt"
```

where `mydataset.nt` is the RDF file to load, you can use all [the formats used by RDF4J](https://rdf4j.org/javadoc/latest/org/eclipse/rdf4j/rio/RDFFormat.html).

### As a dependency

```java
// Create a SPARQL repository
SparqlRepository repository = CompiledSail.compiler().compileToSparqlRepository();
// Init the repository
repository.init();

// ...

// Shutdown the repository (better to release resources)
repository.shutDown();
```

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
- Tweet about the QEndpoint.
- Write interesting articles about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog.

Together, we can make QEndpoint **better**!

## Contributing

First off, thanks for taking the time to contribute! Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make will benefit everybody else and are **greatly appreciated**.

Please read [our contribution guidelines](docs/CONTRIBUTING.md), and thank you for being involved!

## Authors & contributors

The original setup of this repository is by [The QA Company](https://github.com/the-qa-company).

For a full list of all authors and contributors, see [the contributors page](https://github.com/the-qa-company/qEndpoint/contributors).

## Security

QEndpoint follows good practices of security, but 100% security cannot be assured.
QEndpoint is provided **"as is"** without any **warranty**. Use at your own risk.

_For more information and to report security issues, please refer to our [security documentation](docs/SECURITY.md)._

## License

This project is licensed under the **GNU General Public License v3**.

See [LICENSE](LICENSE.md) for more information.
