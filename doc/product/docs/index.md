# Data Engineering & SQL

This page deal about the data engineering project consisting of a Spark & Python application building relations between drugs, scientific publications, pubmed, journals and clinical trials.

For SQL queries part you'll find more details in the [dedicated page](/sql).

----

## Design
We have 5 packages:
 
- entities: have the common abstract & base class for other packages (dependencies for other packages)
- utils: is the functions' toolbox to interact with the outside world (dependencies for other packages)
- json_cleaner: Clean the list of JSON files by creating new files (typically used on serverless like Azure Function)
- data_ingestion: Pipeline that sanitize data, extract drugs and change the data model finally save to a JSON (typically used on Spark cluster like DataBricks)
- feature: Get the list of journals that have the most distinct drugs (typically used on Spark cluster like DataBricks)

The 3 "business" packages should be used with a DAG orchestrator (like Azure DataFactory).
All packages are idempotent & parameterizable.

![Archi!](/assets/images/pipeline_archi.png)

utils & entities are internally published Python packages.
The environment & dependencies mismatch are prevent by pipenv & docker.

As the JSON file management json_cleaner is done in pure Python to ease its re-usability and enable its usage
in parallel on several files it has been separated from the main Spark data_ingestion package.

The data_ingestion package is based on several feature classes in order to enable the re-usability of those feature in
some other Spark pipelines. It is able to run real world scenario where new data arriving by updating existing records 
inserting new ones. In the first place it reads json & csv input files with UTF8 encoding then union data with same name & columns.
Data is read as string to enable parsing before type casting.
Then its parse the remaining unicode string not parsed by encoder, clean the empty string by setting them a null/none cell.
Deduplicate lines considering few columns and merge data from those duplicate.
Cast date column to a uniform format. The data are then persist with the empty titles in order to enable merging with fresh data later (and may be fill the null).
We create a words list from the string/sentences by tokenizing & removing stop words.
Then we extract the drugs from each publication by creating a drug column at the end we have a line per publication/drug.
Finally, we write the data in a JSON file to a format of a list of JSON with one JSON per drug with: 

```json
[{"drug": DrugName,"journal":[["Date","Journal"]],"clinical_trials":[[Date, PublicationID]],"pubmed":[[Date, PublicationID]]},...]
```

## Considerations
If we have To of data we can't store the data in a unique JSON file depending on how we query the data we should then save
it in parquet or in a graph database.

In case of large JSON input for json_cleaner we should leverage an iterative JSON parser (Python package like ijson)

In real world we should set up a whole DevOps approach with branching strategy, cookiecutter , CI, CD, full dockerized environment & quality gates.

We should have secure vault for credentials & proper cloud/software monitoring.

In real world drugs have lots of synonyms with sometimes composed name, 
we should create a drugs reference dataset that could source himself via API to drugs database (like Drugbank) 

To be robust on matching drugs and scalable a search engine like elastic search could be leverage on drugs search in publication.

The artifact folder shouldn't exist as the packages shouldn't be version, it exists only to ease the usage if you can't pull Docker images from my registry.
```shell
docker pull yannick.azurecr.io/drugs/full:latest
```

## Results

The orchestrator is able to run the pipeline with provided parameters.
Note that DataBricks file system need to have a mounted workspace on your data lake.
Unfortunately Azure Data Factory doesn't support yet dockerized activity for Databricks but Function does.

![Drugs!](/assets/images/adf_dbs.png)

The json_cleaner get rid of the trailing comma in the sample data.

Sanitizer well sanitize all the hitches as we can see on the clinical_trial. 
Note the: 

- empty string,
  
- non-consistent date format

- duplicated data with null

- encoding leftover

Before:
![sanitizingBefore!](/assets/images/sanitizingBefore.png)
After:
![sanitizing!](/assets/images/sanitizing.png)

Drugsextractor is able to get all drugs (still an example on clinical_trial):
![drugsextract!](/assets/images/drugsextract.png)

Thanks to the feature package we know that at the most 2 drugs are mentioned in the following journal:

![Result!](/assets/images/end_result.png)

You can launch unit test with nosetests for example on feature:
```shell
nosetests test/test_feature.py
```
![test!](/assets/images/test.png)

To launch locally Spark job use spark-submit for example:
```shell
spark-submit --master local --py-files dist/feature-1.0.0-py3.7.egg feature/main.py "/path_to_params.json/" "/input_path"
```

## Code reference

In the navigation (on your left side) you'll find a section per package that provide the underlying code references.