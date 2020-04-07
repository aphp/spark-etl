package meta

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
                   ) {

    // require(isMultiline.isEmpty || (isMultiline.isDefined && splitFactor.isDefined), "When multiline then splitfactor must be specified too")
    //require(!isMultiline.get && splitFactor.isDefined && splitFactor.get > 1, "When no multiline, splitfactor should be equal to one")
  }

  implicit val colorFormat = yamlFormat6(Schema)
  implicit val paletteFormat = yamlFormat7(Database)

}

