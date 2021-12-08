#!/bin/bash

local_dir="./data"
hdfs_dir="/imdb"
file_to_download=("name.basics" "title.akas" "title.basics" "title.crew" "title.episode" "title.principals" "title.ratings")

if ! dpkg -s wget &> /dev/null; then
    apt-get update &> /dev/null
    apt-get install wget &> /dev/null
fi

if [ ! -d "${local_dir}" ]; then 
    mkdir "${local_dir}"
fi

cd "${local_dir}" || exit

hdfs dfs -mkdir -p "${hdfs_dir}"

for file in "${file_to_download[@]}"
do 
    if [ ! -f "${file}.tsv" ]; then
        echo "Downloading and decompressing ${file}.tsv.gz"
        wget "https://datasets.imdbws.com/${file}.tsv.gz"
        gzip -d "${file}.tsv.gz"
    else
        echo "${file}.tsv already existing locally"
    fi

    if ! hdfs dfs -test -e "${hdfs_dir}/${file}/${file}.tsv"; then
        echo "Tranfering ${file}.tsv to hdfs"
        hdfs dfs -mkdir -p "${hdfs_dir}/${file}"
        hdfs dfs -put -f "${file}.tsv" "${hdfs_dir}/${file}"
    else
        echo "${file}.tsv already existing in hdfs"
    fi
done

echo "Initializing imdb database"
hive -f init-script.hql

