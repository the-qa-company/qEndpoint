<h1 align="center">
  <a href="https://github.com/the-qa-company/qEndpoint">
    <!-- Please provide path to your logo here -->
    <img src="docs/images/qEndpointBanner.png" alt="Logo" width="100%" />
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

[![Pull Requests welcome](https://img.shields.io/badge/PRs-welcome-ff69b4.svg?style=flat-square)](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)

<h3>
  Hey there 
  <img src="https://media.giphy.com/media/hvRJCLFzcasrR4ia7z/giphy.gif" width="25px"/>
</h3>


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
    - [Installers](#installers)
    - [Code](#code)
      - [Back-end](#back-end)
      - [Front-end](#front-end)
- [Usage](#usage)
  - [Docker Image](#docker-image)
    - [`qacompany/qendpoint`](#qacompanyqendpoint)
    - [`qacompany/qendpoint-wikidata`](#qacompanyqendpoint-wikidata)
    - [Useful tools](#useful-tools-1)
  - [Standalone](#standalone)
  - [As a dependency](#as-a-dependency)
- [Connecting with your Wikibase](#connecting-with-your-wikibase)
- [Roadmap](#roadmap)
- [Support](#support)
- [Project assistance](#project-assistance)
- [Contributing](#contributing)
- [Authors \& contributors](#authors--contributors)
- [Security](#security)
- [License](#license)

</details>

---

## About

The qEndpoint is a highly scalable triple store with full-text and  [GeoSPARQL](https://www.ogc.org/standards/geosparql) support. It can be used as a standalone SPARQL endpoint, or as a dependency.
  
### Built With

  <div style="display: flex; justify-content:space-between;">
            <a href="https://spring.io/">
              <figure style="text-align:center; flex:1;">
                  <img src="docs/images/spring logo.png" alt= "spring logo" width="100px" height="68px" />
                  <figcaption styles="text-align: center;">Spring Boot</figcaption>
              </figure>
            </a>
            <a href="https://www.rdfhdt.org/">
              <figure style="text-align:center; flex:1;">
                  <img src="docs/images/rdf-hdt.png" alt= "rdf-hdt logo" width="70px"/>
                  <figcaption styles="text-align: center;">RDF-HDT</figcaption>
              </figure>
            </a>
            <a href="https://rdf4j.org/">
              <figure style="text-align:center; flex:1;">
                  <img src="docs/images/rdf4j.png" alt= "rdf4j logo" width="110px"/>
                  <figcaption styles="text-align: center;">RDF4J</figcaption>
              </figure>
            </a>
  </div>

---

## Getting Started

### Prerequisites

For the backend/benchmark

<ul>
  <h4><li>Java 11 or 17 &nbsp;<img src="docs/images/java logo.png" alt= "Java logo" width="18px"/> </li></h4>
  <h4><li>Maven &nbsp;<img src="docs/images/maven logo.png" alt= "Maven logo" width="40px"/> </li></h4>
</ul>


For the frontend (not mandatory to run the backend)

- see specific [README](./hdt-qs-frontend/README.md)&nbsp; <img src="https://cdn4.iconfinder.com/data/icons/seasonstweetings/256/fall-book-no-text.png" alt= "readme logo" width="18px"/> 

### Installation

#### Installers

Installers for Linux <img src="https://cdn3.iconfinder.com/data/icons/logos-brands-3/24/logo_brand_brands_logos_linux-512.png" alt= "linux logo" width="18px"/> , MacOS <img src="https://cdn2.iconfinder.com/data/icons/social-icons-color/512/apple-512.png" alt= "linux logo" width="18px"/> and Windows <img src="https://cdn1.iconfinder.com/data/icons/logotypes/32/windows-512.png" alt= "windows logo" width="18px"/> &nbsp;can be found [here](https://github.com/the-qa-company/qEndpoint/releases)


#### Code

##### Back-end 

- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the back-end directory `cd hdt-qs-backend`
- Compile the project using this command: `mvn clean install -DskipTests`
- Run the project using `java -jar target/qendpoint-1.2.3-exec.jar` (replace the version by the latest version)

 
  You can use the project as a dependency (replace the version by the latest version) 
&nbsp;<img src="https://cdn4.iconfinder.com/data/icons/pretty_office_3/256/wheel.png" alt= "backend logo" width="18px"/>

```
<dependency>
    <groupId>com.the_qa_company</groupId>
    <artifactId>qendpoint</artifactId>
    <version>1.2.3</version>
</dependency>
```

##### Front-end

- Clone the qEndpoint from this link: `git clone https://github.com/the-qa-company/qEndpoint.git`
- Move to the front-end directory `cd hdt-qs-frontend`
- Install the packages using `npm install`
- Run the project using `npm start`

---

## Usage

### Docker Image

You can use one of our preconfigured Docker images.

#### `qacompany/qendpoint`

**DockerHub**: [qacompany/qendpoint](https://hub.docker.com/r/qacompany/qendpoint)

This Docker 
  image &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/logos-and-brands/512/97_Docker_logo_logos-512.png" alt= "docker logo" width="18px"/>&nbsp;contains the endpoint, you can upload your dataset and start using it.

You just have to run the image and it will prepare the environment &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/Vista-Inspirate_1.0/128x128/apps/advancedsettings.png" alt= "setup logo" width="18px"/>&nbsp; by downloading &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/unicons-line-vol-2/24/cloud-download-512.png" alt= "download logo" width="18px"/>&nbsp; the index and setting up &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/flat-christmas-icons-1/75/_magic_wand-512.png" alt= "magicwand logo" width="18px"/>&nbsp; the repository using the snippet below:

```bash
docker run -p 1234:1234 --name qendpoint qacompany/qendpoint
```

You can also specify the size of the memory &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/cloud-computing-71/64/Cloud_Success-512.png" alt= "memory logo" width="18px"/> &nbsp;allocated by setting the docker environnement &nbsp; <img src="https://cdn3.iconfinder.com/data/icons/construction-138/200/Wrench_And_Screwdriver-512.png" alt= "docker env logo" width="18px"/>&nbsp;value _MEM_SIZE_. By default this value is set to 6G. You should not set this value below 4G because you will certainly run out of memory with large dataset &nbsp; <img src="https://cdn2.iconfinder.com/data/icons/database-73/24/mining_database_web_apps_database_color_f-1024.png" alt= "dataset logo" width="18px"/>&nbsp;. For bigger dataset, a bigger value is also recommended for big dataset, as an example, [Wikidata-all](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) won't run without at least 10G.

```bash
docker run -p 1234:1234 --name qendpoint --env MEM_SIZE=6G qacompany/qendpoint
```

You can stop the container and rerun &nbsp;<img src="https://cdn0.iconfinder.com/data/icons/navigation-2-3/32/Navigation_Loading-1024.png" alt= "rerun logo" width="18px"/>&nbsp; it at anytime maintaining the data inside (qendpoint is the name of the container) using the following commands:

```bash
docker stop qendpoint
docker start qendpoint
```

<img src="https://cdn4.iconfinder.com/data/icons/essentials-volume-2/128/wrote-note-2-512.png" alt= "note logo" width="18px"/>&nbsp;: **Note** this container may occupy a huge portion of the disk due to the size of the data index, so make sure to delete the container if you don't need it anymore by using the command below:

```bash
docker rm qendpoint
```

#### `qacompany/qendpoint-wikidata`

**DockerHub**: [qacompany/qendpoint-wikidata](https://hub.docker.com/r/qacompany/qendpoint-wikidata)

This Docker image &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/logos-and-brands/512/97_Docker_logo_logos-512.png" alt= "docker logo" width="18px"/>&nbsp; contains the endpoint with a script to download &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/smallicons-documents/32/archive_arrow_down-1024.png" alt= "download logo" width="18px"/>&nbsp;  an index containing the [Wikidata Truthy](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements from our servers&nbsp;<img src="https://cdn2.iconfinder.com/data/icons/leto-blue-big-data/64/big_data_data_analytics-46-512.png" alt= "server logo" width="18px"/>, so you simply have to wait for the index download and start using it.

You just have to run the image and it will prepare the environment by downloading the index and setting up the repository using the code below <img src="https://cdn3.iconfinder.com/data/icons/creative-and-idea/500/Idea-thinking-think-concept_10-512.png" alt= "server logo" width="18px"/>:

```bash
docker run -p 1234:1234 --name qendpoint-wikidata qacompany/qendpoint-wikidata
```

You can also specify the size of the memory &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/cloud-computing-71/64/Cloud_Success-512.png" alt= "memory logo" width="18px"/> &nbsp;allocated by setting the docker environnement&nbsp;<img src="https://cdn3.iconfinder.com/data/icons/construction-138/200/Wrench_And_Screwdriver-512.png" alt= "docker env logo" width="18px"/>&nbsp; value _MEM_SIZE_. By default this value is set to 6G, a bigger value is also recommended for big dataset&nbsp; <img src="https://cdn2.iconfinder.com/data/icons/database-73/24/mining_database_web_apps_database_color_f-1024.png" alt= "dataset logo" width="18px"/>, as an example, [Wikidata-all](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) won't run without at least 10G.

```bash
docker run -p 1234:1234 --name qendpoint-wikidata --env MEM_SIZE=6G qacompany/qendpoint-wikidata
```

You can specify the dataset to download using the environnement value _HDT_BASE_, by default the value is `wikidata_truthy`, but the current available values are:

- `wikidata_truthy` - [Wikidata Truthy](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements (need at least `6G` of memory)
- `wikidata_all` - [Wikidata-all ](https://www.wikidata.org/wiki/Wikidata:Database_download#RDF_dumps) statements (need at least `10G` of memory)

```bash
docker run -p 1234:1234 --name qendpoint-wikidata --env MEM_SIZE=10G --env HDT_BASE=wikidata_all qacompany/qendpoint-wikidata
```

You can stop the container and rerun <img src="https://cdn0.iconfinder.com/data/icons/navigation-2-3/32/Navigation_Loading-1024.png" alt= "rerun logo" width="18px"/>&nbsp; it at anytime maintaining the data inside (qendpoint is the name of the container) using the below code:

```bash
docker stop qendpoint-wikidata
docker start qendpoint-wikidata
```

<img src="https://cdn4.iconfinder.com/data/icons/essentials-volume-2/128/wrote-note-2-512.png" alt= "note logo" width="18px"/>&nbsp;: **Note** this container may occupy a huge portion of the disk due to the size of the data index, so make sure to delete <img src="https://cdn2.iconfinder.com/data/icons/ios-7-tab-bar-icons/500/trash-512.png" alt= "delete logo" width="18px"/>&nbsp;the container if you don't need it anymore using the command as shown below:

```bash
docker rm qendpoint-wikidata
```

#### Useful tools

You can access http://localhost:1234 where there is a GUI where you can write SPARQL queries and execute them, and there is the RESTful API 
&nbsp;<img src="https://cdn0.iconfinder.com/data/icons/buno-api/32/api_software_integration-512.png" alt= "api logo" width="18px"/>&nbsp; available which you can use to run queries from any application over HTTP like so:

```bash
curl -H 'Accept: application/sparql-results+json' localhost:1234/api/endpoint/sparql --data-urlencode 'query=select * where{ ?s ?p ?o } limit 10'
```

<img src="https://cdn4.iconfinder.com/data/icons/essentials-volume-2/128/wrote-note-2-512.png" alt= "note logo" width="18px"/>&nbsp;: **Note** first query will take some time in order to map the index to memory, later on it will be much faster!

Most of the result formats are available, you can use for example:

- JSON: `application/sparql-results+json`
- XML: `application/sparql-results+xml`
- Binary RDF: `application/x-binary-rdf-results-table`

### Standalone

You can run the endpoint with this command:

```bash
java -jar endpoint.jar &
```

you can find a template of [the application.properties file in the backend source](hdt-qs-backend/src/main/resources/application.properties)

If you have the HDT file &nbsp;<img src="https://cdn0.iconfinder.com/data/icons/delivery-e-commerce-bluetone/91/DeliveryEcommerce-22-512.png" alt= "hdt logo" width="18px"/>&nbsp; of your graph, you can put it before loading the endpoint in the hdt-store directory (by default `hdt-store/index_dev.hdt`)

If you don't have the HDT, you can upload the dataset &nbsp;<img src="https://cdn2.iconfinder.com/data/icons/elastic-search-filled-outline-1/128/Elastic_Search_36_-_Filled_Outline_-_36-13-512.png" alt= "data logo" width="18px"/>&nbsp; to the endpoint by running the command while the endpoint is running:

```bash
curl "http://127.0.0.1:1234/api/endpoint/load" -F "file=@mydataset.nt"
```

where `mydataset.nt` is the RDF file &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/blockchain-8/48/icon0008_decentralize-512.png" alt= "rdf logo" width="18px"/>&nbsp; to load, you can use all [the formats used by RDF4J](https://rdf4j.org/javadoc/latest/org/eclipse/rdf4j/rio/RDFFormat.html).

### As a dependency

You can create a SPARQL repository &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/digital-services-2/512/Node-512.png" alt= "sparql repo" width="18px"/>&nbsp; using this method, don't forget to init the repository

```java
// Create a SPARQL repository
SparqlRepository repository = CompiledSail.compiler().compileToSparqlRepository();
// Init the repository
repository.init();
```

You can execute SPARQL queries &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/digital-services-2/512/Node-512.png" alt= "sparql repo" width="18px"/>&nbsp; using the `executeTupleQuery`, `executeBooleanQuery`, `executeGraphQuery` or `execute`.

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

Don't forget to shutdown &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/education-flat-1-1/48/21-512.png" alt= "shutdown" width="18px"/>&nbsp; the repository after usage

```java
// Shutdown the repository (better to release resources)
repository.shutDown();
```

You can get the RDF4J &nbsp;<img src="https://cdn1.iconfinder.com/data/icons/blockchain-8/48/icon0008_decentralize-512.png" alt= "rdf logo" width="18px"/>&nbsp; repository with the `getRepository()` method.

```java
// get the rdf4j repository (if required)
SailRepository rdf4jRepo = repository.getRepository();
```
---

## Connecting with your Wikibase

- run the qEndpoint locally <img src="https://cdn2.iconfinder.com/data/icons/videoplayer/1000/Loop-512.png" alt= "run" width="20px"/>
- `cd wikibase` &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/office-and-business-conceptual-flat/169/43-512.png" alt= "cd dir" width="18px"/>
- move the file `prefixes.sparql` to your qEndpoint installation &nbsp;<img src="https://cdn2.iconfinder.com/data/icons/bitsies/128/Enter-1024.png" alt= "transfer" width="18px"/>
- (re-)start your endpoint to use the prefixes &nbsp;<img src="https://cdn2.iconfinder.com/data/icons/metro-uinvert-dock/256/Power_-_Restart.png" alt= "transfer" width="18px"/>
- run <img src="https://cdn2.iconfinder.com/data/icons/videoplayer/1000/Loop-512.png" alt= "run" width="20px"/>
- `cd wikibase` &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/office-and-business-conceptual-flat/169/43-512.png" alt= "cd dir" width="18px"/>

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
<img src="https://cdn4.iconfinder.com/data/icons/pretty_office_3/256/wheel.png" alt= "backend logo" width="18px"/>&nbsp; 

---
## Roadmap

See the [open issues](https://github.com/the-qa-company/qEndpoint/issues) for a list of proposed features (and known issues).

- [Top Feature Requests](https://github.com/the-qa-company/qEndpoint/issues?q=label%3Aenhancement+is%3Aopen+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Top Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aissue+is%3Aopen+label%3Abug+sort%3Areactions-%2B1-desc) (Add your votes using the 👍 reaction)
- [Newest Bugs](https://github.com/the-qa-company/qEndpoint/issues?q=is%3Aopen+is%3Aissue+label%3Abug)

---

## Support

Reach out to the maintainer at one of the following places:

- [GitHub issues](https://github.com/the-qa-company/qEndpoint/issues/new?assignees=&labels=question&template=04_SUPPORT_QUESTION.md&title=support%3A+)&nbsp; <img src="https://cdn2.iconfinder.com/data/icons/social-icons-33/128/Github-512.png" alt= "backend logo" width="18px"/>
- Contact options listed on [this GitHub profile](https://github.com/the-qa-company)&nbsp; <img src="https://cdn3.iconfinder.com/data/icons/leto-user-group/64/__user_person_profile-512.png" alt= "backend logo" width="18px"/>
- [The QA Company website](https://the-qa-company.com/) &nbsp; <img src="docs/images/logo.svg" alt= "backend logo" width="18px"/>

---

## Project assistance

If you want to say **thank you** or/and support active development of qEndpoint:

- Add a [GitHub Star](https://github.com/the-qa-company/qEndpoint) to the project &nbsp; <img src="https://cdn4.iconfinder.com/data/icons/small-n-flat/24/star-512.png" alt= "star" width="18px"/>
- [Tweet](https://twitter.com/TheQACompany) about the qEndpoint &nbsp; <img src="https://cdn2.iconfinder.com/data/icons/social-media-2285/512/1_Twitter_colored_svg-512.png" alt= "star" width="18px"/>
- Write interesting articles about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog &nbsp;<img src="https://cdn2.iconfinder.com/data/icons/flat-seo-web-ikooni/128/flat_seo2-32-512.png" alt= "star" width="18px"/>

<img src="https://cdn2.iconfinder.com/data/icons/happiness-flat/60/Thumbs-Up-like-favourite-liked-hand-512.png" alt= "thumbsup" width="18px"/>&nbsp;
  Together, we can make qEndpoint **better** !
&nbsp;<img src="https://cdn0.iconfinder.com/data/icons/social-reaction-and-emoji/519/happy-512.png" alt= "feedback" width="18px"/>

---

## Contributing

First of all, **thanks** for taking the time to contribute! Contributions &nbsp;<img src="https://cdn4.iconfinder.com/data/icons/thank-you/256/Artboard_5_copy-512.png" alt= "feedback" width="18px"/>&nbsp; are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make will benefit everybody else and are **greatly appreciated**.

Please read [our contribution guidelines](docs/CONTRIBUTING.md), and thank you for being involved!

---

## Authors & contributors

The original setup of this repository is by [The QA Company](https://github.com/the-qa-company).

For a full list of all authors and contributors, see [the contributors page](https://github.com/the-qa-company/qEndpoint/contributors).

---

## Security

qEndpoint follows good practices of security&nbsp;<img src="https://cdn4.iconfinder.com/data/icons/essentials-72/24/008_-_Lock-512.png" alt= "feedback" width="18px"/>, but 100% security cannot be assured.
qEndpoint is provided **"as is"** without any **warranty**. Use at your own risk.

_For more information and to report security issues, please refer to our [security documentation](docs/SECURITY.md)._

---

## License

This project is licensed under the **GNU General Public License v3**.

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