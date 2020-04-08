package io.frama.parisni.spark.meta

trait GetTables {

  val SQL_HIVE_TABLE: String =
    """
       select
             '%s' lib_database,
             d."NAME" lib_schema,
             t."TBL_NAME" lib_table,
             "TBL_TYPE" typ_table,
             numrow."PARAM_VALUE"::bigint count_table,
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  last_analyze,
             "COLUMN_NAME" lib_column,
             "TYPE_NAME" typ_column,
             "INTEGER_IDX" + 1 order_column,
             dist."PARAM_VALUE"::bigint count_distinct_column,
             (100 * nc."PARAM_VALUE"::bigint / numrow."PARAM_VALUE"::bigint)::float null_ratio_column,
             "COMMENT" comment_fonctionnel,
              cast(null as boolean) as is_mandatory,
              null::text as comment_fonctionnel_column
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
      	  left join "TABLE_PARAMS" dist ON (dist."TBL_ID" = t."TBL_ID" AND dist."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.distinctCount' AND dist."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" nc ON (nc."TBL_ID" = t."TBL_ID" AND nc."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.nullCount' AND nc."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE NOT ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
    """

  val SQL_HIVE_TABLE_EXT: String =
    """
       select
             '%s' lib_database,
             d."NAME" lib_schema,
             t."TBL_NAME" lib_table,
             "TBL_TYPE" typ_table,
             numrow."PARAM_VALUE"::bigint count_table,
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  last_analyze,
             "COMMENT" comment_fonctionnel,
             sch."PARAM_VALUE" as schem,
             sts.stts
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
          left join (select "TBL_ID", array_agg("PARAM_KEY"||'='||"PARAM_VALUE") as stts from "TABLE_PARAMS" where "PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.distinctCount|spark.sql.statistics.colStats.\w+.nullCount' group by "TBL_ID") sts
          on sts."TBL_ID" = t."TBL_ID"
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
    """

  val SQL_PG_VIEW: String =
    """
       select '%s' as lib_database,
      t.table_schema as lib_schema,
             t.table_name as lib_table,
             'view' as typ_table,
             null::bigint as count_table,
             null::timestamp as last_analyze,
             c.column_name lib_column,
             c.data_type typ_column,
             c.ordinal_position order_column
        from information_schema.tables t
      	 left join information_schema.columns c
      	     on t.table_schema = c.table_schema
      	     and t.table_name = c.table_name
       where table_type = 'VIEW'
         and t.table_schema not in ('information_schema', 'pg_catalog')
       order by 1,2,3, order_column
    """.stripMargin

  val SQL_PG_TABLE =
    """
       SELECT '%s' as lib_database,
             n.nspname AS lib_schema,
             c.relname AS lib_table,
             'physical' AS typ_table,
             co.rowcount AS count_table,
             co.last_analyze,
             f.attname::text AS lib_column,
             format_type(f.atttypid, f.atttypmod) AS typ_column,
             attnum::int as order_column,
             null_frac::float as null_ratio_column,
             n_distinct::bigint as count_distinct_column,
             obj_description(g.oid) as comment_fonctionnel,
             f.attnotnull AS is_mandatory,
             i.relname AS index_name,
             CASE
             WHEN i.oid <> 0::oid THEN 't'::text
             ELSE 'f'::text
             END AS is_index,
             CASE
             WHEN p.contype = 'p'::"char" THEN 't'::text
             ELSE 'f'::text
             END AS primarykey,
             CASE
             WHEN p.contype = 'u'::"char" THEN 't'::text
             WHEN p.contype = 'p'::"char" THEN 't'::text
             ELSE 'f'::text
             END AS uniquekey,
             col_description(('"'|| n.nspname || '"."' || c.relname ||'"')::regclass, attnum::int) as comment_fonctionnel_column
        FROM pg_attribute f
      	 JOIN pg_class c ON c.oid = f.attrelid
      	 JOIN pg_type t ON t.oid = f.atttypid
      	 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
      	 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      	 LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND (f.attnum = ANY (p.conkey))
      	 LEFT JOIN pg_class g ON p.confrelid = g.oid
      	 LEFT JOIN pg_index ix ON (f.attnum = ANY (ix.indkey::smallint[])) AND c.oid = f.attrelid AND c.oid = ix.indrelid
      	 LEFT JOIN pg_class i ON ix.indexrelid = i.oid
      	 left join (SELECT relname, schemaname, n_live_tup as rowcount, coalesce(last_autoanalyze, last_analyze) last_analyze FROM pg_stat_all_tables) co on (co.relname = c.relname and co.schemaname = n.nspname)
         left join pg_stats sts on (sts.schemaname = n.nspname AND sts.tablename = c.relname AND sts.attname = f.attname)
       WHERE c.relkind::text = 'r'::character(1)::text AND f.attnum > 0
         and n.nspname not in ('information_schema', 'pg_catalog')
       ORDER BY lib_schema, lib_table, order_column
    """
}
