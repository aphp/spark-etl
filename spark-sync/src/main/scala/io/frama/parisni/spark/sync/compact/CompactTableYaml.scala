package io.frama.parisni.spark.sync.compact

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

object CompactTableYaml extends DefaultYamlProtocol {

  case class Database(jobName: String
    , deltaPath: String
    , partition: Option[String]
    , numFiles: Int
    , host:String
    , port:String
    , user:String
    , schema:String
    , db:String
    , pw:Option[String])
  {
    //require(numThread.isDefined && (numThread.get < 9 && numThread.get > 0), "Thread number should be between 1 and 8")
    //
  }

  implicit val paletteFormat = yamlFormat10(Database)

}
