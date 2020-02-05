# wagtail
An approach to managing nested JSON data by flattening keys with an example implemention scripts in Apache Solr and Python

The examples are taken from British railway infrastructure Train Planning System (TPS) and Working Timetable (WTT) Common Interface File (CIF) data sets

### Apache Solr
Apache Lucene (Solr)[https://lucene.apache.org/solr/]

### docker
docker [docker](https://www.docker.com/)

## Dependencies
### jq
[jq](https://stedolan.github.io/jq/)

### parallel
GNU [parallel](https://www.gnu.org/software/parallel/)

### docker
[docker](https://www.docker.com/)

### python
[python](https://www.python.org/)

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

## WTT
From the `CIF` project directory
```console
$ cd CIF
$ sh ../bin/create-cluster.sh
```

```console
$ ./run.sh
```

## Implementation

### flatten-keys

This package is heavily based on the python [flatten-dict](https://github.com/ianlini/flatten-dict) package modified to produce "`.`" separated keys 

### solr

This is a simple implentation of the Apache Solr based on the python [requests](https://requests.readthedocs.io/en/master/) package looking to implement v2 API calls where available and supporting `core` and `collections` Solr mode
