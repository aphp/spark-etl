package io.frama.parisni.spark.compat

import org.apache.spark.sql.DataFrame


// Provide code-compatibility with Spark 1.6 and older 2.x
// Some operations will fail at runtime though
package object compat {
    import scala.language.implicitConversions

    class AugmentedDF(df: DataFrame) {
        def union(o: DataFrame): DataFrame = throw new NotImplementedError("union needs spark 2.0")

        def unionByName(o: DataFrame): DataFrame =  // From Spark 2.3
            if (o.columns.sameElements(df.columns)) df.union(o) // no difference in columns
            else                                    df.union(o.select(df.columns.head, df.columns.tail :_*))

        def crossJoin(o: DataFrame): DataFrame = df.join(o) // would throw at runtime in spark 2
    }

    implicit def augmentDf(df: DataFrame): AugmentedDF = new AugmentedDF(df)
}
