# Sail compiler schema

```turtle
@prefix mdlc: <http://the-qa-company.com/modelcompiler/>
```

- [``mdlc:acceptNoLanguageLiterals``](#mdlcacceptnolanguageliterals)
- [``mdlc:debugDisableOptionReloading``](#mdlcdebugdisableoptionreloading)
- [``mdlc:debugShowPlan``](#mdlcdebugshowplan)
- [``mdlc:debugShowQueryResultCount``](#mdlcdebugshowqueryresultcount)
- [``mdlc:debugShowTime``](#mdlcdebugshowtime)
- [``mdlc:dirLocation``](#mdlcdirlocation)
- [``mdlc:endpointStoreStorage``](#mdlcendpointstorestorage)
- [``mdlc:filterNode``](#mdlcfilternode)
- [``mdlc:hdtLoadReadMode``](#mdlchdtloadreadmode)
- [``mdlc:hdtMapReadMode``](#mdlchdtmapreadmode)
- [``mdlc:hdtOnePassMode``](#mdlchdtonepassmode)
- [``mdlc:hdtPassMode``](#mdlchdtpassmode)
- [``mdlc:hdtReadMode``](#mdlchdtreadmode)
- [``mdlc:hdtTwoPassMode``](#mdlchdttwopassmode)
- [``mdlc:languageFilter``](#mdlclanguagefilter)
- [``mdlc:languageFilterLang``](#mdlclanguagefilterlang)
- [``mdlc:linkedSailNode``](#mdlclinkedsailnode)
- [``mdlc:lmdbStoreStorage``](#mdlclmdbstorestorage)
- [``mdlc:luceneEvalMode``](#mdlcluceneevalmode)
- [``mdlc:luceneLang``](#mdlclucenelang)
- [``mdlc:luceneNode``](#mdlclucenenode)
- [``mdlc:luceneParam``](#mdlcluceneparam)
- [``mdlc:luceneReindexQuery``](#mdlclucenereindexquery)
- [``mdlc:main``](#mdlcmain)
- [``mdlc:memoryStoreStorage``](#mdlcmemorystorestorage)
- [``mdlc:multiFilterNode``](#mdlcmultifilternode)
- [``mdlc:nativeStoreStorage``](#mdlcnativestorestorage)
- [``mdlc:node``](#mdlcnode)
- [``mdlc:noOptimization``](#mdlcnooptimization)
- [``mdlc:option``](#mdlcoption)
- [``mdlc:paramFilter``](#mdlcparamfilter)
- [``mdlc:paramFilterAnd``](#mdlcparamfilterand)
- [``mdlc:paramFilterOr``](#mdlcparamfilteror)
- [``mdlc:paramKey``](#mdlcparamkey)
- [``mdlc:paramLink``](#mdlcparamlink)
- [``mdlc:paramValue``](#mdlcparamvalue)
- [``mdlc:parsedString``](#mdlcparsedstring)
- [``mdlc:parsedStringParam``](#mdlcparsedstringparam)
- [``mdlc:predicateFilter``](#mdlcpredicatefilter)
- [``mdlc:rdfStoreSplit``](#mdlcrdfstoresplit)
- [``mdlc:storageMode``](#mdlcstoragemode)
- [``mdlc:type``](#mdlctype)
- [``mdlc:typeFilter``](#mdlctypefilter)
- [``mdlc:typeFilterLuceneExp``](#mdlctypefilterluceneexp)
- [``mdlc:typeFilterObject``](#mdlctypefilterobject)
- [``mdlc:typeFilterPredicate``](#mdlctypefilterpredicate)

## `mdlc:acceptNoLanguageLiterals`

**IRI**: [http://the-qa-company.com/modelcompiler/acceptNoLanguageLiterals](http://the-qa-company.com/modelcompiler/acceptNoLanguageLiterals)

### Description

Describe the type language Filter param

---

## `mdlc:debugDisableOptionReloading`

**IRI**: [http://the-qa-company.com/modelcompiler/debugDisableOptionReloading](http://the-qa-company.com/modelcompiler/debugDisableOptionReloading)

### Description

Disable option reloading

---

## `mdlc:debugShowPlan`

**IRI**: [http://the-qa-company.com/modelcompiler/debugShowPlan](http://the-qa-company.com/modelcompiler/debugShowPlan)

### Description

Show query plans

---

## `mdlc:debugShowQueryResultCount`

**IRI**: [http://the-qa-company.com/modelcompiler/debugShowQueryResultCount](http://the-qa-company.com/modelcompiler/debugShowQueryResultCount)

### Description

Show query count

---

## `mdlc:debugShowTime`

**IRI**: [http://the-qa-company.com/modelcompiler/debugShowTime](http://the-qa-company.com/modelcompiler/debugShowTime)

### Description

Show exec time of query

---

## `mdlc:dirLocation`

**IRI**: [http://the-qa-company.com/modelcompiler/dirLocation](http://the-qa-company.com/modelcompiler/dirLocation)

### Description

Describe a directory

---

## `mdlc:endpointStoreStorage`

**IRI**: [http://the-qa-company.com/modelcompiler/endpointStoreStorage](http://the-qa-company.com/modelcompiler/endpointStoreStorage)

### Description

The storage mode endpoint store

---

## `mdlc:filterNode`

**IRI**: [http://the-qa-company.com/modelcompiler/filterNode](http://the-qa-company.com/modelcompiler/filterNode)

### Description

Describe the type Filter

---

## `mdlc:hdtLoadReadMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtLoadReadMode](http://the-qa-company.com/modelcompiler/hdtLoadReadMode)

### Description

The hdt load reading mode, load the full HDT into memory

---

## `mdlc:hdtMapReadMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtMapReadMode](http://the-qa-company.com/modelcompiler/hdtMapReadMode)

### Description

The hdt load reading mode, map the HDT into memory

---

## `mdlc:hdtOnePassMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtOnePassMode](http://the-qa-company.com/modelcompiler/hdtOnePassMode)

### Description

The mode to parse the Triple flux in one pass, reduce disk usage

---

## `mdlc:hdtPassMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtPassMode](http://the-qa-company.com/modelcompiler/hdtPassMode)

### Description

The mode to parse the Triple flux

### Values

Default value: [mdlc:hdtTwoPassMode](#mdlchdttwopassmode)

Usable value(s) for this property:

- [mdlc:hdtOnePassMode](#mdlchdtonepassmode)
- [mdlc:hdtTwoPassMode](#mdlchdttwopassmode)

---

## `mdlc:hdtReadMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtReadMode](http://the-qa-company.com/modelcompiler/hdtReadMode)

### Description

The hdt reading mode

### Values

Default value: [mdlc:hdtMapReadMode](#mdlchdtmapreadmode)

Usable value(s) for this property:

- [mdlc:hdtLoadReadMode](#mdlchdtloadreadmode)
- [mdlc:hdtMapReadMode](#mdlchdtmapreadmode)

---

## `mdlc:hdtTwoPassMode`

**IRI**: [http://the-qa-company.com/modelcompiler/hdtTwoPassMode](http://the-qa-company.com/modelcompiler/hdtTwoPassMode)

### Description

The mode to parse the Triple flux in two passes, reduce time usage

---

## `mdlc:languageFilter`

**IRI**: [http://the-qa-company.com/modelcompiler/languageFilter](http://the-qa-company.com/modelcompiler/languageFilter)

### Description

Describe the type language Filter

---

## `mdlc:languageFilterLang`

**IRI**: [http://the-qa-company.com/modelcompiler/languageFilterLang](http://the-qa-company.com/modelcompiler/languageFilterLang)

### Description

Describe the type language Filter param

---

## `mdlc:linkedSailNode`

**IRI**: [http://the-qa-company.com/modelcompiler/linkedSailNode](http://the-qa-company.com/modelcompiler/linkedSailNode)

### Description

Describe the type Linked sail

---

## `mdlc:lmdbStoreStorage`

**IRI**: [http://the-qa-company.com/modelcompiler/lmdbStoreStorage](http://the-qa-company.com/modelcompiler/lmdbStoreStorage)

### Description

The storage mode lmdb

---

## `mdlc:luceneEvalMode`

**IRI**: [http://the-qa-company.com/modelcompiler/luceneEvalMode](http://the-qa-company.com/modelcompiler/luceneEvalMode)

### Description

Describe the Lucene evaluation mode

---

## `mdlc:luceneLang`

**IRI**: [http://the-qa-company.com/modelcompiler/luceneLang](http://the-qa-company.com/modelcompiler/luceneLang)

### Description

Describe the Lucene language(s)

---

## `mdlc:luceneNode`

**IRI**: [http://the-qa-company.com/modelcompiler/luceneNode](http://the-qa-company.com/modelcompiler/luceneNode)

### Description

Describe the type Lucene

---

## `mdlc:luceneParam`

**IRI**: [http://the-qa-company.com/modelcompiler/luceneParam](http://the-qa-company.com/modelcompiler/luceneParam)

### Description

Describe a Lucene param

---

## `mdlc:luceneReindexQuery`

**IRI**: [http://the-qa-company.com/modelcompiler/luceneReindexQuery](http://the-qa-company.com/modelcompiler/luceneReindexQuery)

### Description

Describe the Lucene reindex query

---

## `mdlc:main`

**IRI**: [http://the-qa-company.com/modelcompiler/main](http://the-qa-company.com/modelcompiler/main)

### Description

The main node, the start point of the model

---

## `mdlc:memoryStoreStorage`

**IRI**: [http://the-qa-company.com/modelcompiler/memoryStoreStorage](http://the-qa-company.com/modelcompiler/memoryStoreStorage)

### Description

The storage mode memory store

---

## `mdlc:multiFilterNode`

**IRI**: [http://the-qa-company.com/modelcompiler/multiFilterNode](http://the-qa-company.com/modelcompiler/multiFilterNode)

### Description

Describe the type Multi-Filter

---

## `mdlc:nativeStoreStorage`

**IRI**: [http://the-qa-company.com/modelcompiler/nativeStoreStorage](http://the-qa-company.com/modelcompiler/nativeStoreStorage)

### Description

The storage mode native store

---

## `mdlc:node`

**IRI**: [http://the-qa-company.com/modelcompiler/node](http://the-qa-company.com/modelcompiler/node)

### Description

Describe a node

---

## `mdlc:noOptimization`

**IRI**: [http://the-qa-company.com/modelcompiler/noOptimization](http://the-qa-company.com/modelcompiler/noOptimization)

### Description

Disable optimization for native stores

---

## `mdlc:option`

**IRI**: [http://the-qa-company.com/modelcompiler/option](http://the-qa-company.com/modelcompiler/option)

### Description

option predicate

### Values

Usable value(s) for this property:

- [mdlc:debugDisableOptionReloading](#mdlcdebugdisableoptionreloading)
- [mdlc:debugShowPlan](#mdlcdebugshowplan)
- [mdlc:debugShowQueryResultCount](#mdlcdebugshowqueryresultcount)
- [mdlc:debugShowTime](#mdlcdebugshowtime)
- [mdlc:noOptimization](#mdlcnooptimization)

---

## `mdlc:paramFilter`

**IRI**: [http://the-qa-company.com/modelcompiler/paramFilter](http://the-qa-company.com/modelcompiler/paramFilter)

### Description

Describe a node param filter

---

## `mdlc:paramFilterAnd`

**IRI**: [http://the-qa-company.com/modelcompiler/paramFilterAnd](http://the-qa-company.com/modelcompiler/paramFilterAnd)

### Description

Describe a node param filter op AND

---

## `mdlc:paramFilterOr`

**IRI**: [http://the-qa-company.com/modelcompiler/paramFilterOr](http://the-qa-company.com/modelcompiler/paramFilterOr)

### Description

Describe a node param filter op OR

---

## `mdlc:paramKey`

**IRI**: [http://the-qa-company.com/modelcompiler/paramKey](http://the-qa-company.com/modelcompiler/paramKey)

### Description

Describe a parameter key

---

## `mdlc:paramLink`

**IRI**: [http://the-qa-company.com/modelcompiler/paramLink](http://the-qa-company.com/modelcompiler/paramLink)

### Description

Describe a node param linked sails

---

## `mdlc:paramValue`

**IRI**: [http://the-qa-company.com/modelcompiler/paramValue](http://the-qa-company.com/modelcompiler/paramValue)

### Description

Describe a parameter value

---

## `mdlc:parsedString`

**IRI**: [http://the-qa-company.com/modelcompiler/parsedString](http://the-qa-company.com/modelcompiler/parsedString)

### Description

Describe a parsed string datatype

---

## `mdlc:parsedStringParam`

**IRI**: [http://the-qa-company.com/modelcompiler/parsedStringParam](http://the-qa-company.com/modelcompiler/parsedStringParam)

### Description

Describe a parsed string param

---

## `mdlc:predicateFilter`

**IRI**: [http://the-qa-company.com/modelcompiler/predicateFilter](http://the-qa-company.com/modelcompiler/predicateFilter)

### Description

Describe the type predicate Filter

---

## `mdlc:rdfStoreSplit`

**IRI**: [http://the-qa-company.com/modelcompiler/rdfStoreSplit](http://the-qa-company.com/modelcompiler/rdfStoreSplit)

### Description

The storage load split update count

### Value

Number value

- default value: 1000
- min value: 1

---

## `mdlc:storageMode`

**IRI**: [http://the-qa-company.com/modelcompiler/storageMode](http://the-qa-company.com/modelcompiler/storageMode)

### Description

The storage mode

### Values

Default value: [mdlc:endpointStoreStorage](#mdlcendpointstorestorage)

Usable value(s) for this property:

- [mdlc:endpointStoreStorage](#mdlcendpointstorestorage)
- [mdlc:lmdbStoreStorage](#mdlclmdbstorestorage)
- [mdlc:memoryStoreStorage](#mdlcmemorystorestorage)
- [mdlc:nativeStoreStorage](#mdlcnativestorestorage)

---

## `mdlc:type`

**IRI**: [http://the-qa-company.com/modelcompiler/type](http://the-qa-company.com/modelcompiler/type)

### Description

Describe the type of a node

---

## `mdlc:typeFilter`

**IRI**: [http://the-qa-company.com/modelcompiler/typeFilter](http://the-qa-company.com/modelcompiler/typeFilter)

### Description

Describe the predicate type type

---

## `mdlc:typeFilterLuceneExp`

**IRI**: [http://the-qa-company.com/modelcompiler/typeFilterLuceneExp](http://the-qa-company.com/modelcompiler/typeFilterLuceneExp)

### Description

Describe the lucene exp predicate type

---

## `mdlc:typeFilterObject`

**IRI**: [http://the-qa-company.com/modelcompiler/typeFilterObject](http://the-qa-company.com/modelcompiler/typeFilterObject)

### Description

Describe the predicate type type param object

---

## `mdlc:typeFilterPredicate`

**IRI**: [http://the-qa-company.com/modelcompiler/typeFilterPredicate](http://the-qa-company.com/modelcompiler/typeFilterPredicate)

### Description

Describe the predicate type type param type predicate

---

