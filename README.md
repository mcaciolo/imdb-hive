# Imdb-hive

## Introduction
This repository is a demo example of using Hive to store and query data from the [IMDb dataset](https://datasets.imdbws.com/):
- a containered version, which run a containered version of Hadoop and Hive can be execute locally
- a Google Cloud Platform version, which can be deployed in a GCP project with a cloud storage bucket and a DataProc cluster 

## Containered version of Hive
- From the main directory of the repository, run docker-compose : `docker-compose up -d`.
- Once all the containers deploied, run the script on the hive-server container : `docker container exec -it hive-server /scripts/container-script.sh`. This will download all the files, transfer them to hdfs and create external and internal tables.
- Run the test queries in the hive-server container : `docker container exec -it hive-server hive -f /scripts/queries.hql`

## GCP version
- From a machine 