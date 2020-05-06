# How to run

This is a handy way of synchronizing between postgres, solr, spark and delta using simple yaml
files.

# pgpass File

To synchronize with postgres tables, a `.pgpass` file with the below content needs to be added to your `home` directory with `600`:

```
yourHost:yourPort:yourDb:yourUser:yourPassword
```

# Yaml io.frama.parisni.spark.sync.copy.PostgresToDelta file

Example yaml file:

``` yaml
jobName:        Postgres To Delta
hostPg:         localhost
portPg:         5432
databasePg:     postgres
userPg:         postgres
timestampLastColumn:  date_update
timestampColumns: [date_update, date_update2, date_update3]
dateMax:  "2018-12-01 00:00:00"
tables:
    - tablePg: source_pg
      tableHive: target_delta
      schemaPg: public
      schemaHive: /tmp
      typeLoad: scd1
      key: [id, pk2]
      numThread: 4
      isActive: true
```

# Yaml io.frama.parisni.spark.sync.copy.DeltaToSolr File

Example yaml file:

``` yaml
jobName:        Delta To Solr
timestampLastColumn:  date_update
timestampColumns: [date_update, date_update2, date_update3]
dateMax:  "2018-01-01 00:00:00"
tables:
    - tableSolr: targetsolr_fromdelta
      tableDelta: source
      ZkHost: zkhost
      schemaDelta: /tmp
      key: [id, pk2]
      numThread: 4
      isActive: true
```

