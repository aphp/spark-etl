package io.frama.parisni.spark.meta.extractor

import io.frama.parisni.spark.meta.ConfigMetaYaml

object Utils {


  /**
   * This method allow to load an FeatureExtractTrait class from the Strategy specified in Yaml configuration file
   */
  def getFeatureExtractImplClass(strategy : Option[ConfigMetaYaml.ExtractStrategy]): FeatureExtractTrait={
    getFeatureExtractImplClass( getExtractStrategyFromYaml(strategy).featureExtractImplClass.get)
  }

  /**
   * This method allow to load an FeatureExtractTrait class by name
   *
   * @param className class name should by format as "PackageName.ClassName"
   *
   * @return The class if the class name is correct, null otherwise
   */
  private def getFeatureExtractImplClass(className: String): FeatureExtractTrait = {
    val featureClass = getClass.getClassLoader.loadClass(className).newInstance()
    featureClass match {
      case extractTrait: FeatureExtractTrait => extractTrait
      case _ => null
    }
  }

  /**
   * Analyse the input meta extraction strategy and return a full strategy is some fields are unfilled
   *
   * @param strategyFromConf
   *
   * @return a complete meta extraction strategy or null if strategy is wrong
   */
   private def getExtractStrategyFromYaml(strategyFromConf: Option[ConfigMetaYaml.ExtractStrategy]): ConfigMetaYaml.ExtractStrategy = {

    val strategy: ConfigMetaYaml.ExtractStrategy = strategyFromConf.getOrElse(ConfigMetaYaml.ExtractStrategy())

    //If strategy has not been provide in configuration file then provide the default strategy
    if (strategyFromConf.isEmpty) {
      strategy
    } else {
      //Check field by field if all fields are well filled
      //If the field is empty then we put the default value

      //Checking field "featureExtractImplClass"
      val featureExtract:String = strategy.featureExtractImplClass.getOrElse(ConfigMetaYaml.ExtractStrategy().featureExtractImplClass.get)

      //return the strategy with empty field replaced by default value
      ConfigMetaYaml.ExtractStrategy(Some(featureExtract))
    }
  }


}
