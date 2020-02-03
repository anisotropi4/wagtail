# wagtail
An approach to managing nested JSON data by flattening keys with an example implemention scripts in Solr and Python

The examples are taken from British railway infrastructure Train Planning System (TPS) and Common Interface File (CIF) data sets

## Dependencies
### jq
https://stedolan.github.io/jq/

### parallel
https://www.gnu.org/software/parallel/

### python
https://www.python.org/

$ virtualenv venv
$ source venv/bin/activate
$ pip install docker-compose pandas lxml xmltodict

### docker-compose
https://docs.docker.com/compose/

### pandas
https://pandas.pydata.org

### lxml
https://lxml.de/

### xmltodict
https://github.com/martinblech/xmltodict

## TPS
From the `wagtail` project directory
$ cd TPS
$ sh ../bin/create-cluster.sh
$ ./run.sh

## CIF
From the `CIF` project directory
$ cd CIF
$ sh ../bin/create-cluster.sh
$ ./run.sh
