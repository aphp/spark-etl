package io.frama.parisni.spark.sync.copy

import com.lucidworks.spark.LazyLogging
import org.scalatest.{FunSuite, Outcome}

/**
 * Base abstract class for all Scala unit tests in spark-solr for handling common functionality.
 *
 * Copied from SparkFunSuite, which is inaccessible from here.
 */
trait SparkSolrFunSuite extends FunSuite with LazyLogging {

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("com.lucidworks.spark", "c.l.s")
    try {
      logger.info(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logger.info(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

}
