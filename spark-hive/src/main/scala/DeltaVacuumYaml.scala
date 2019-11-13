package io.frama.parisni.spark.hive

import net.jcazevedo.moultingyaml._

object DeltaVacuumYaml extends DefaultYamlProtocol {

  case class Database(
                       jobName: String
                       , pathDelta: String
                       , tables: Option[List[Table]]
                     )

  case class Table(
                    tableDelta: String
                    , isActive: Option[Boolean] = Some(true)
                    , nbHours: Option[Double]
                  ) {

    // require(isMultiline.isEmpty || (isMultiline.isDefined && splitFactor.isDefined), "When multiline then splitfactor must be specified too")
    // require(!isMultiline.get && splitFactor.isDefined && splitFactor.get > 1, "When no multiline, splitfactor should be equal to one")
    // require(numThread.isDefined && (numThread.get < 9 && numThread.get > 0), "Thread number should be between 1 and 8")
  }

  implicit val colorFormat = yamlFormat3(Table)
  implicit val paletteFormat = yamlFormat3(Database)

}

