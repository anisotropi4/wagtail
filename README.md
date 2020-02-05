# wagtail

An approach to managing nested JSON data by flattening keys with an example implemention scripts in Apache Solr and Python

The examples are taken from British railway infrastructure Train Planning System (TPS) and Common Interface File (CIF) data sets

## Background

The reason for this framework is a sudden realisation in late December 2019 that there are issues with the way in which document store [NoSQL](https://en.wikipedia.org/wiki/NoSQL) databases manage nested [JSON](https://www.json.org/json-en.html)

### Key flattening

The approach chosen was to flattend nested keys to a "`.`" separated value. This maybe more clearly seen in the following two examples:

```javascript
{
  "id": "64",
  "lastmodified": "2016-01-30T07:27:53",
  "directed": {
    "start": {
      "point": {
        "lineid": "0", "nodeid": "165732"
      }
    },
    "end": {
      "point": {
        "lineid": "0", "nodeid": "165738"
      }
    }
  }
}
```
When transformed becomes:
```javascript
{
  "edge.id":"64",
  "lastmodified":"2016-01-30T07:27:53",
  "directed.start.point.lineid":"0",
  "directed.start.point.nodeid":"165732",
  "directed.end.point.lineid":"0",
  "directed.end.point.nodeid":"165738"
}
```

Nested array block are transformed into separate arrays but where index order is maintained as follows:
```javascript
{
  "blockid": "946",
  "lastmodified": "2019-05-30T10:27:57",
  "way": {
    "point": [
      {"lineid": "0", "nodeid": "304235"},
      {"lineid": "1", "nodeid": "207759"},
      {"lineid": "2", "nodeid": "304122"}
    ]
  }
}
```
When transformed becomes:
```javascript
{
  "blockid": "946",
  "lastmodified": "2019-05-30T10:27:57",
  "way.point.lineid": ["0", "2", "2" ],
  "way.point.nodeid": ["304235", "207759", "304122" ],
  "id": "0002.00000002"
}
```

This approach is based on a customised version [flatten-dict](https://github.com/ianlini/flatten-dict) package implemented in [python](https://www.python.org/)

## Data sources and processing

The example data are from the Network Rail [open-data](https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/) Infrastructure and Schedule feeds. The Infrastructure model is a largely undocument [XML](https://www.w3.org/XML/) format whereas the Schedule is based on data distribured in the Common Interface File (CIF) End User Specification (Version 29)

Scripts to convert data from [HTML](https://en.wikipedia.org/wiki/HTML) and XML to JSON as well as manage and create Apache [Solr](https://lucene.apache.org/solr/) [docker](https://www.docker.com/) containers are provided

License information for this data is given at the bottom of this document

## Software components



### Apache Solr

Apache [Solr](https://lucene.apache.org/solr/) is an open-source `#NoSQL` search system based on Apache [Lucene](https://lucene.apache.org/). Solr provides distributed indexing, replication and load-balanced of queries, automated failover and recovery, centralized configuration. 

### Apache Zookeeper
Apache [Zookeeper](https://zookeeper.apache.org/) is an open-source distributed management system that uses a hierarchical key-value store to provide a distributed configuration system service.

### docker

[docker](https://www.docker.com/) is a platform as a service (PaaS) that use OS-level virtualization to deliver container based application and configuration.

This installation guide is based on Docker installation notes on [debian](https://docs.docker.com/install/linux/docker-ce/debian/) or Mint and/or [ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

## Dependencies
### jq
[jq](https://stedolan.github.io/jq/)

### parallel
GNU [parallel](https://www.gnu.org/software/parallel/)

### docker solr
[docker](https://www.docker.com/)

[docker-solr](https://github.com/docker-solr/docker-solr)

### python modules


```console
$ virtualenv venv
$ source venv/bin/activate
$ pip install docker-compose pandas lxml xmltodict
```

### docker-compose
python [docker-compose](https://docs.docker.com/compose/)

### pandas
python [pandas](https://pandas.pydata.org)

### lxml
python [lxml](https://lxml.de/)

### xmltodict
python [xmltodict](https://github.com/martinblech/xmltodict)

## TPS
From the `wagtail` project directory
```console
$ cd TPS
$ sh ../bin/create-cluster.sh
```

```console
$ ./run.sh
```

## CIF
From the `CIF` project directory
```console
$ cd CIF
$ sh ../bin/create-cluster.sh
```

```console
$ ./run.sh
```

## Implementation

### app.flatten-keys

This package is heavily based on the python [flatten-dict](https://github.com/ianlini/flatten-dict) package modified to produce "`.`" separated keys 

### app.solr

This is a simple implentation of the Apache Solr based on the python [requests](https://requests.readthedocs.io/en/master/) package looking to implement v2 API calls where available and supporting `core` and `collections` Solr mode

## Note and licensing

This software framework and scripts are released under an `MIT License` without  warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose and noninfringement

It is based on access and processing the data released by Network Rail Infrastructure Limited licensed under the following [licence](www.networkrail.co.uk/data-feeds/terms-and-conditions)

This implementation is based on access to the [opentraintimes](https://networkrail.opendata.opentraintimes.com/) mirror of the Network Rail open [datafeeds](https://datafeeds.networkrail.co.uk) for ease of access

I would like to thank both [Network Rail](https://www.networkrail.co.uk/) for making this data available and [Open Train Times](https://www.opentraintimes.com/) for their mirror of this data
