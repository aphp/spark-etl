# Meta !

[git public repository][git-repo]

The tool fetches data model from postgres and hive by exploiting metadata stored in efficient structure internally. It stores the result in a simple and historized data model stored in a postgresql database for later use. It can manage large infrastructure with light impact on databases.

Usage:
- MPD generation
- database maintenance/monitoring
- fresh documentation

## Running the App

The entry point of this App is the MetaSync Object, which needs at least 2 input arguments:

- The path to the YAML configuration file
- The log level to apply to the spark context

##### configuration file
The YAML configuration file give information about which database, which schemas to analyse and also which extraction 
strategy to apply to them.

###### configuration file example


``` yaml
jobName:    Eds Stats Calc
hostPg:     0.0.0.0
portPg:     5432
databasePg: metadb
schemaPg:   meta
userPg:     superman
schemas:
  - dbName: cohort-prod
    dbType: postgresql
    host: 0.0.0.0
    db: omop_prod
    schemaRegexFilter: omop
    user: batman
    isActive: true
  - dbName: spark-prod
    dbType: spark
    schemaRegexFilter: coronaomop_unstable
    host: 0.0.0.0 #postgres metastore host
    db: hive
    user: robin
    isActive: true
    strategy:
        extractor:
            featureExtractImplClass: io.frama.parisni.spark.meta.TestFeatureExtractImpl
        generator:
            tableGeneratorImplClass: io.frama.parisni.spark.meta.TestTableGeneratorImpl
    user: etl
```

##### Log level
The log level should be one of this String value: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

## Postgres configuration

When plugged to a postgres database, one of the metadata column provided by this tool is the `last_commit_timestampz`.<br>
This column gives information about when the last update occurred for a specific table.

In order to get this column filled you need to activate the `track_commit_timestamp` in your postgres configuration file 
and restart it.

In 'postgres.conf' change ```#track_commit_timestamp = off``` into ```track_commit_timestamp = on```

You can then check the status of this parameter by querying your database as below 

```sql
show track_commit_timestamp;
```



[git-repo]: <https://framagit.org/parisni/spark-etl/-/tree/master/spark-meta>