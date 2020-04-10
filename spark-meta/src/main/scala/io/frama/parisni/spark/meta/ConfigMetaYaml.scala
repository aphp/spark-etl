package io.frama.parisni.spark.meta

import net.jcazevedo.moultingyaml._

object ConfigMetaYaml extends DefaultYamlProtocol {

  case class Database(jobName: String
                      , hostPg: String
                      , portPg: Int
                      , databasePg: String
                      , userPg: String
                      , schemaPg: String
                      , schemas: Option[List[Schema]])

  case class Schema(dbName: String
                    , dbType: String
                    , host: String
                    , db: String
                    , user: String
                    , isActive: Option[Boolean]
                    , extractor: Option[ExtractStrategy]
                   ) {
  }

  /**
   * This class contain the meta extraction strategy fields to adopt
   * Udpate this class if others strategy elements are needed
   *
   * @note All fields have to be filled by a default value in case no strategy is specified by the user
   *       Be careful when moving Class package since the path is hardcoded in default value
   */
  case class ExtractStrategy(featureExtractImplClass: Option[String] = Some("io.frama.parisni.spark.meta.extractor.DefaultFeatureExtractImpl"))

  implicit val primaryColorFormat = yamlFormat1(ExtractStrategy)
  implicit val colorFormat = yamlFormat7(Schema)
  implicit val paletteFormat = yamlFormat7(Database)

}

