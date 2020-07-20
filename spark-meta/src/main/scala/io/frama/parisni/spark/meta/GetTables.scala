package io.frama.parisni.spark.meta

trait GetTables {

  val SQL_HIVE_TABLE: String =
    s"""
       select
             '%s' ${Constants.LIB_DATABASE},
             d."NAME" ${Constants.LIB_SCHEMA},
             t."TBL_NAME" ${Constants.LIB_TABLE},
             "TBL_TYPE" ${Constants.TYP_TABLE},
             numrow."PARAM_VALUE"::bigint ${Constants.COUNT_TABLE},
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  ${Constants.LAST_ANALYSE},
             "COLUMN_NAME" ${Constants.LIB_COLUMN},
             "TYPE_NAME" ${Constants.TYP_COLUMN},
             "INTEGER_IDX" + 1 ${Constants.ORDER_COLUMN},
             dist."PARAM_VALUE"::bigint ${Constants.COUNT_DISTINCT_COLUMN},
             (100 * nc."PARAM_VALUE"::bigint / numrow."PARAM_VALUE"::bigint)::float ${Constants.NULL_RATIO_COLUMN},
             "COMMENT" ${Constants.COMMENT_FONCTIONNEL},
              cast(null as boolean) as ${Constants.IS_MANDATORY},
              null::text as ${Constants.COMMENT_FONCTIONNEL_COLUMN},
              null::boolean ${Constants.IS_INDEX},
              null::timestamp as ${Constants.LAST_COMMIT_TIMESTAMPZ}
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
      	  left join "TABLE_PARAMS" dist ON (dist."TBL_ID" = t."TBL_ID" AND dist."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\\w+.distinctCount' AND dist."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" nc ON (nc."TBL_ID" = t."TBL_ID" AND nc."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\\w+.nullCount' AND nc."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE NOT ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
    """

  //TODO check that \\w+ (ie. in 'spark.sql.statistics.colStats.\\w+.distinctCount') works

  val SQL_HIVE_TABLE_EXT: String =
    s"""
       select
             '%s' ${Constants.LIB_DATABASE},
             d."NAME" ${Constants.LIB_SCHEMA},
             t."TBL_NAME" ${Constants.LIB_TABLE},
             "TBL_TYPE" ${Constants.TYP_TABLE},
             numrow."PARAM_VALUE"::bigint ${Constants.COUNT_TABLE},
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  ${Constants.LAST_ANALYSE},
             "COMMENT" ${Constants.COMMENT_FONCTIONNEL},
             sch."PARAM_VALUE" as ${Constants.SCHEM},
             sts.${Constants.STTS},
             null::boolean ${Constants.IS_INDEX}
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
          left join (select "TBL_ID", array_agg("PARAM_KEY"||'='||"PARAM_VALUE") as ${Constants.STTS} from "TABLE_PARAMS" where "PARAM_KEY" ~ 'spark.sql.statistics.colStats.\\w+.distinctCount|spark.sql.statistics.colStats.\\w+.nullCount' group by "TBL_ID") sts
          on sts."TBL_ID" = t."TBL_ID"
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
    """

  val SQL_PG_VIEW: String =
    s"""
       select '%s' as ${Constants.LIB_DATABASE},
      t.table_schema as ${Constants.LIB_SCHEMA},
             t.table_name as ${Constants.LIB_TABLE},
             'view' as ${Constants.TYP_TABLE},
             null::bigint as ${Constants.COUNT_TABLE},
             null::timestamp as ${Constants.LAST_ANALYSE},
             c.column_name ${Constants.LIB_COLUMN},
             c.data_type ${Constants.TYP_COLUMN},
             c.ordinal_position ${Constants.ORDER_COLUMN},
             null::boolean ${Constants.IS_INDEX}
        from information_schema.tables t
      	 left join information_schema.columns c
      	     on t.table_schema = c.table_schema
      	     and t.table_name = c.table_name
       where table_type = 'VIEW'
         and t.table_schema not in ('information_schema', 'pg_catalog')
       order by 1,2,3, ${Constants.ORDER_COLUMN}
    """

  val SQL_PG_TABLE =
    s"""
       SELECT '%s' as ${Constants.LIB_DATABASE},
             n.nspname AS ${Constants.LIB_SCHEMA},
             c.relname AS ${Constants.LIB_TABLE},
             'physical' AS ${Constants.TYP_TABLE},
             co.rowcount AS ${Constants.COUNT_TABLE},
             co.${Constants.LAST_ANALYSE},
             f.attname::text AS ${Constants.LIB_COLUMN},
             format_type(f.atttypid, f.atttypmod) AS ${Constants.TYP_COLUMN},
             attnum::int as ${Constants.ORDER_COLUMN},
             null_frac::float as ${Constants.NULL_RATIO_COLUMN},
             n_distinct::bigint as ${Constants.COUNT_DISTINCT_COLUMN},
             obj_description(g.oid) as ${Constants.COMMENT_FONCTIONNEL},
             f.attnotnull AS ${Constants.IS_MANDATORY},
             i.relname AS ${Constants.INDEX_NAME},
             CASE
             WHEN i.oid <> 0::oid THEN true
             ELSE false
             END AS ${Constants.IS_INDEX},
             CASE
             WHEN p.contype = 'p'::"char" THEN true
             ELSE false
             END AS ${Constants.PRIMARY_KEY},
             CASE
             WHEN p.contype = 'u'::"char" THEN true
             WHEN p.contype = 'p'::"char" THEN true
             ELSE false
             END AS ${Constants.UNIQUE_KEY},
             col_description(('"'|| n.nspname || '"."' || c.relname ||'"')::regclass, attnum::int) as ${Constants.COMMENT_FONCTIONNEL_COLUMN}
        FROM pg_attribute f
      	 JOIN pg_class c ON c.oid = f.attrelid
      	 JOIN pg_type t ON t.oid = f.atttypid
      	 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
      	 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      	 LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND (f.attnum = ANY (p.conkey))
      	 LEFT JOIN pg_class g ON p.confrelid = g.oid
      	 LEFT JOIN pg_index ix ON (f.attnum = ANY (ix.indkey::smallint[])) AND c.oid = f.attrelid AND c.oid = ix.indrelid
      	 LEFT JOIN pg_class i ON ix.indexrelid = i.oid
      	 left join (SELECT relname, schemaname, n_live_tup as rowcount, coalesce(last_autoanalyze, last_analyze) ${Constants.LAST_ANALYSE} FROM pg_stat_all_tables) co on (co.relname = c.relname and co.schemaname = n.nspname)
         left join pg_stats sts on (sts.schemaname = n.nspname AND sts.tablename = c.relname AND sts.attname = f.attname)
       WHERE c.relkind::text = 'r'::character(1)::text AND f.attnum > 0
         and n.nspname not in ('information_schema', 'pg_catalog')
       ORDER BY ${Constants.LIB_SCHEMA}, ${Constants.LIB_TABLE}, ${Constants.ORDER_COLUMN}
    """
}

object GetTables extends GetTables
