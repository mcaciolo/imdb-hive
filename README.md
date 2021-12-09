# Imdb-hive

## Introduction
This repository is a demo example of using Apache Hive to store and query data from the [IMDb dataset](https://datasets.imdbws.com/), in two versions :
- a containered version, which run on a containered version of Hadoop and Hive and can be execute locally
- a Google Cloud Platform version, which can be deployed in a GCP project with a cloud storage bucket and a DataProc cluster 

### Table initialization
The bash script *container-script.sh* (or *gcp-script.sh* for the GCP version) initialize the hive database with 7 external tables, reading directly from the 7 raw tsv files, and 7 corresponding internal tables in parquet format, clustered and partitioned in order to optimize the queries.

### Test script
The hive script *queries.hql* execute the same three test queries in the external and optimized tables, showing the better performance of optimized tables.

## Containered version of Hive
- From the main directory of the repository, run docker-compose : `docker-compose up -d`.
- Once all the containers deploied, run the script on the hive-server container : `docker container exec -it hive-server /scripts/container-script.sh`. This will download all the files, transfer them to hdfs and create external and internal tables.
- Run the test queries in the hive-server container : `docker container exec -it hive-server hive -f /scripts/queries.hql`

## GCP version
The following instructions should be executed from a machine with gcloud installed and with enough rights to access to the bucket and to submit a job to the Dataproc instance :
- From the main directory of the repository, run the bash script ./scripts/gcp-script.sh, with the next four arguments :
-  