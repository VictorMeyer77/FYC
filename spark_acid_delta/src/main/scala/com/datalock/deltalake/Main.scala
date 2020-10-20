package com.datalock.deltalake

import org.apache.spark.sql._

object Main{

    val FILE_PATH = "/tmp/spark/spark_acid_delta"

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("spark_acid_delta")
          .getOrCreate()

        // --- écriture et lecture d'un dataframe au format delta --- //

        val df = spark.range(200)

        df.write
          .format("delta")
          .mode("append")
          .save(FILE_PATH)

        val df2 = spark.read
                    .format("delta")
                    .load(FILE_PATH)

        println(df2.count())

        // --- remplacement et lecture du dataframe en provoquant volontairement une erreur --- //

        import spark.implicits._

        val df3 = spark.range(200)
        df3.map { i => {
            if (i > 10) {
                i / 0
            }
            i
        }
        }.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(FILE_PATH)

        val df4 = spark.read
          .format("delta")
          .load(FILE_PATH)

        println(df4.count())

    }
}