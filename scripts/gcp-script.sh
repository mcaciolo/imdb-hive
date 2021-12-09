#!/bin/bash

local_dir=$1
gs_dir=$2
file_to_download=("name.basics" "title.akas" "title.basics" "title.crew" "title.episode" "title.principals" "title.ratings")

if [ ! -d "${local_dir}" ]; then 
    mkdir "${local_dir}"
fi

cd "${local_dir}" || exit

for file in "${file_to_download[@]}"
do 
    if ! gsutil stat "${gs_dir}/${file}/${file}.tsv"; then
        if [ ! -f "${file}.tsv" ]; then
            echo "Downloading and decompressing ${file}.tsv.gz"
            wget "https://datasets.imdbws.com/${file}.tsv.gz"
            gzip -d "${file}.tsv.gz"
        else
            echo "${file}.tsv already existing locally"
        fi
        echo "Tranfering ${file}.tsv to gs"
        gsutil cp "${file}.tsv" "${gs_dir}/${file}/"
    else
        echo "${file}.tsv already existing in hdfs"
    fi
done