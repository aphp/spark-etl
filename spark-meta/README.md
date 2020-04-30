# Meta !

The tool fetches data model from postgres and hive by exploiting metadata stored in efficient structure internally. It stores the result in a simple and historized data model stored in a postgresql database for later use. It can manage large infrastructure with light impact on databases.

Usage:
- MPD generation
- database maintenance/monitoring
- fresh documentation


# configuration file example


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
