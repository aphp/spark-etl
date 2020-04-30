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
                    , schemaRegexFilter: Option[String]
                    , user: String
                    , isActive: Option[Boolean]
                    , strategy: Option[Strategy])

  /**
    * This class contain the meta extractor and generator strategies to adopt
    * Udpate this class if others strategy elements are needed
    *
    * @note All fields have to be filled by a default value in case no strategy is specified by the user
    *       Be careful when moving Class package since the path is hardcoded in default value
    * @param extractor The extractor strategy to apply
    * @param generator The generator strategy to apply
    */
  case class Strategy(extractor: Option[ExtractStrategy] = Some(ExtractStrategy())
                      , generator: Option[TableGeneratorStrategy] = Some(TableGeneratorStrategy())
                     )

  /**
    * This class define witch 'FeatureExtractTrait' strategy to apply
    * @param featureExtractImplClass the path to the targeted class.
    *                                path should be format like 'packageName.ClassName'
    *                                Default value will be the default strategy to apply
    */
  case class ExtractStrategy(featureExtractImplClass: String = "io.frama.parisni.spark.meta.strategy.extractor.DefaultFeatureExtractImpl")

  /**
    * This class define witch 'TableGeneratorTrait' strategy to apply
    * @param tableGeneratorImplClass the path to the targeted class.
    *                                path should be format like 'packageName.ClassName'
    *                                Default value will be the default strategy to apply
    */
  case class TableGeneratorStrategy(tableGeneratorImplClass: String = "io.frama.parisni.spark.meta.strategy.generator.DefaultTableGeneratorImpl")

  //YAML implicit format mapping
  implicit val extractorFormat = yamlFormat1(ExtractStrategy)
  implicit val generatorFormat = yamlFormat1(TableGeneratorStrategy)
  implicit val strategyFormat = yamlFormat2(Strategy)
  implicit val schemaFormat = yamlFormat8(Schema)
  implicit val databaseFormat = yamlFormat7(Database)

}

