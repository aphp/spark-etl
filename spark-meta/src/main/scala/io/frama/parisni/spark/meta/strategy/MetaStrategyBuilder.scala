package io.frama.parisni.spark.meta.strategy

import io.frama.parisni.spark.meta.ConfigMetaYaml
import io.frama.parisni.spark.meta.ConfigMetaYaml.{ExtractStrategy, TableGeneratorStrategy}
import io.frama.parisni.spark.meta.strategy.extractor.FeatureExtractTrait
import io.frama.parisni.spark.meta.strategy.generator.TableGeneratorTrait

object MetaStrategyBuilder {

  /**
    * Get a per default MetaStrategy
    */
  def build():MetaStrategy={
    build(ConfigMetaYaml.Strategy())
  }

  /**
    * Get a custom MetaStrategy
    */
  def build(strategy: Option[ConfigMetaYaml.Strategy]):MetaStrategy={
    //Get the specified strategy or load the per default strategy
    build(strategy.getOrElse(ConfigMetaYaml.Strategy()))
  }

  /**
    * Get a custom MetaStrategy
    */
  def build(strategy: ConfigMetaYaml.Strategy):MetaStrategy={
    val extractor:FeatureExtractTrait = getExtractorClass(strategy.extractor.getOrElse(ExtractStrategy()).featureExtractImplClass)
    val generator:TableGeneratorTrait = getGeneratorClass(strategy.generator.getOrElse(TableGeneratorStrategy()).tableGeneratorImplClass)

    new MetaStrategy(extractor,generator)
  }

  private def getExtractorClass(className:String):FeatureExtractTrait={
    val extractorClass = getClass.getClassLoader.loadClass(className).newInstance()
    extractorClass match {
      case extractorTrait: FeatureExtractTrait => extractorTrait
      case _ => null
    }
  }

  private def getGeneratorClass(className:String):TableGeneratorTrait={
    val generatorClass = getClass.getClassLoader.loadClass(className).newInstance()
    generatorClass match {
      case generatorTrait: TableGeneratorTrait => generatorTrait
      case _ => null
    }
  }

}
