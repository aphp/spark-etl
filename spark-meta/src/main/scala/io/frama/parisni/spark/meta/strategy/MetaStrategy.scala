package io.frama.parisni.spark.meta.strategy

import io.frama.parisni.spark.meta.strategy.extractor.FeatureExtractTrait
import io.frama.parisni.spark.meta.strategy.generator.TableGeneratorTrait

class MetaStrategy(
    val extractor: FeatureExtractTrait,
    val generator: TableGeneratorTrait
) {

  override def toString: String =
    "extractor:" + extractor + "\ngenerator:" + generator
}
