DROP DATABASE IF EXISTS imdb CASCADE;

CREATE DATABASE  imdb;

USE imdb;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.lazysimple.extended_boolean_literal=true;


-- Creating external tables from hdfs tsv files

CREATE EXTERNAL TABLE  title_akas_ext
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    language STRING,
    types ARRAY<STRING>,
    attributes ARRAY<STRING>,
    isOriginalTitle BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.akas/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  title_basics_ext
(
    tconst STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult BOOLEAN,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.basics/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  title_crew_ext
(
    tconst STRING,
    directors ARRAY<STRING>,
    writers ARRAY<STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.crew/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  title_episode_ext
(
    tconst STRING,
    parentTconst STRING,
    seasonNumber INT,
    episodeNumber INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.episode/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  title_principals_ext
(
    tconst STRING,
    ordering INT,
    nconst STRING,
    category STRING,
    job STRING,
    characters STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.principals/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  title_ratings_ext
(
    tconst STRING,
    averageRating DOUBLE,
    numVotes INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}title.ratings/'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE  name_basics_ext
(
    nconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession ARRAY<STRING>,
    knownForTitles ARRAY<STRING> 
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS TERMINATED BY ","
LINES TERMINATED BY "\n"
STORED AS TEXTFILE
LOCATION '${DATAPATH}name.basics/'
tblproperties ("skip.header.line.count"="1");

-- Creating hive tables in parquet format

CREATE TABLE  title_akas
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    language STRING,
    types ARRAY<STRING>,
    attributes ARRAY<STRING>,
    isOriginalTitle BOOLEAN
)
CLUSTERED BY (region, language) INTO 16 BUCKETS
STORED AS PARQUET;

CREATE TABLE  title_basics
(
    tconst STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult BOOLEAN,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres ARRAY<STRING>
)
CLUSTERED BY (startYear) INTO 16 BUCKETS
STORED AS PARQUET;

CREATE TABLE  title_crew
(
    tconst STRING,
    directors STRING,
    writers STRING
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE  title_episode
(
    tconst STRING,
    parentTconst STRING,
    seasonNumber INT,
    episodeNumber INT
)
STORED AS PARQUET;

CREATE TABLE  title_principals
(
    tconst STRING,
    ordering INT,
    nconst STRING,
    job STRING,
    characters STRING
)
PARTITIONED BY (category STRING)
STORED AS PARQUET;

CREATE TABLE  title_ratings
(
    tconst STRING,
    averageRating DOUBLE,
    numVotes INT
)
STORED AS PARQUET;

CREATE TABLE  name_basics
(
    nconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession ARRAY<STRING>,
    knownForTitles ARRAY<STRING> 
)
CLUSTERED BY (birthYear, deathYear) INTO 16 BUCKETS
STORED AS PARQUET;


-- Inserting data to tables

INSERT INTO TABLE title_akas
SELECT 
    titleId,
    ordering,
    title,
    region,
    language,
    types,
    attributes,
    isOriginalTitle
FROM title_akas_ext;

INSERT INTO TABLE title_basics
SELECT 
    tconst,
    titleType,
    primaryTitle,
    originalTitle,
    isAdult,
    startYear,
    endYear,
    runtimeMinutes,
    genres
FROM title_basics_ext;

INSERT INTO TABLE title_crew
SELECT 
    tconst,
    directors_2,
    writers_2
FROM title_crew_ext
LATERAL VIEW EXPLODE(directors) dirs AS directors_2
LATERAL VIEW EXPLODE(writers) wrts AS writers_2;

INSERT INTO TABLE title_episode
SELECT 
    tconst,
    parentTconst,
    seasonNumber,
    episodeNumber
FROM title_episode_ext;

INSERT INTO TABLE title_principals
PARTITION(category)
SELECT 
    tconst,
    ordering,
    nconst,
    job,
    characters,
    category
FROM title_principals_ext
DISTRIBUTE BY category;

INSERT INTO TABLE title_ratings
SELECT
    tconst,
    averageRating,
    numVotes
FROM title_ratings_ext;

INSERT INTO TABLE name_basics
SELECT
    nconst,
    primaryName,
    birthYear,
    deathYear,
    primaryProfession,
    knownForTitles
FROM name_basics_ext
DISTRIBUTE BY (birthYear, deathYear);


