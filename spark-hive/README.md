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
      numThread: 4
      updateDatetime: toto
      deleteDatetime: toto
      isDelete: true
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

