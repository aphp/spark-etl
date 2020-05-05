package io.frama.parisni.spark.sync.analyze

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

object AnalyzeTableYaml extends DefaultYamlProtocol {

  case class Database(jobName: String
    , deltaPath: String
    , host:String
    , port:String
    , user:String
    , schema:String
    , db:String
    , pw:Option[String])
  {
    //require(numThread.isDefined && (numThread.get < 9 && numThread.get > 0), "Thread number should be between 1 and 8")
  }

  implicit val paletteFormat = yamlFormat8(Database)

}
