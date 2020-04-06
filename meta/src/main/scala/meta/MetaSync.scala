/**
 * This file is part of SPARK-OMOP.
 *
 * SPARK-OMOP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPARK-OMOP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SPARK-OMOP.  If not, see <https://www.gnu.org/licenses/>.
 */
package meta

import com.typesafe.scalalogging.LazyLogging
import meta.ConfigMetaYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

object MetaSync extends App with LazyLogging {

  val YAML = args(0)
  val LOG = args(1)


  val spark = SparkSession.builder()
    .appName("cohort sync")
    .getOrCreate()

  spark.sparkContext.setLogLevel(LOG)

  run(spark, YAML)

  def run(spark: SparkSession, yamlFilePath: String): Unit = {
    // for each datasource

    val ymlTxt = Source.fromFile(yamlFilePath).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[Database]


    try {
      for (source <- database.schemas.getOrElse(Nil)) {
        if (source.isActive.getOrElse(true)) {
          // get the information
          val extract = new MetaExtractor(spark, source.host, source.db, source.user, source.dbType)
          extract.initTables(source.dbName)
          // write to db
          val load = new MetaLoader(database.hostPg, database.databasePg, database.schemaPg, database.userPg)
          load.loadDatabase(extract.getDatabase, source.dbName)
          load.loadSchema(extract.getSchema, source.dbName)
          load.loadTable(extract.getTable, source.dbName)
          load.loadColumn(extract.getColumn, source.dbName)
          load.loadReference(extract.getReference, source.dbName)
        }
      }
    }
  }
}

