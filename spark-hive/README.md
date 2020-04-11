# How to run

This is handy way of bridging postgres, hive, spark and delta with simple yaml
files.

# Yaml HiveToPostgres file


Example yaml file:

``` yaml
jobName:      hive To postgres
hostPg:         host
portPg:         5432
databasePg:     edsr
userPg:         etl
schemaPg:       public
tables:
    - tableHive: test_scd_hive
      tablePg: test_scd_pg
      key: [id, cd]
      hash: hash
      schemaHive: edsdev
      bulkLoadMode: stream # stream or csv; default:stream
      numThread: 4
      reindex: true # will deactivate indexes first and reindex afterwards
      deleteSet: delete_date = now() # each row not present in the hive table will be tagged to delete with this statement
      active: true # the table will be loaded; default:true
```

Complex yaml exampe with join:

``` yaml
# this allows to fetch data from postgres and then join them on the hive side to get some columns. This is usefull when postgres manages the primary keys with sequences.
jobName:      hive To postgres
hostPg:         host
portPg:         5432
databasePg:     edsr
userPg:         etl
schemaPg:       public
tables:
    - tableHive: test_scd_hive
      tablePg: test_scd_pg
      key: [id, cd]
      hash: hash
      schemaHive: edsdev
      bulkLoadMode: stream # stream or csv; default:stream
      numThread: 4
      joinTable: cohort_definition # the postgres table to fetch from
      joinFetchColumns: [cohort_definition_id] # the column to fetch and add to hive
      joinPostgresColumn: cohort_definition_source_value # the postgres column to join with
      joinHiveColumn: cohort_definition_source_value # the hive column to join with
      joinKeepColumn: false # keep the columns from the joined table
      active: true # the table will be loaded; default:true
```

# pgpass File


Add a `.pgpass` file into your `home` with `600`:

```
yourHost:yourPort:yourDb:yourUser:yourPassword
```

# Yaml PostgresToHive File



``` yaml
jobName:      hive To postgres
hostPg:         host
portPg:         5432
databasePg:     edsr
userPg:         etl
tables:
    - tablePg: test_scd_pg
      tableHive: test_scd_hive
      schemaPg: public
      schemaHive: edsdev
      key: id
      numThread: 8
      isActive: false
```

