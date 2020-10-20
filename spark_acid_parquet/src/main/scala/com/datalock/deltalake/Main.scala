package com.datalock.deltalake

import org.apache.spark.sql._

object Main{

    val FILE_PATH = "/tmp/spark/spark_acid_parquet"

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("spark_acid_parquet")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        // --- écriture et lecture d'un dataframe au format parquet --- //

        val df = spark.range(200)

        df.write
          .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .mode("append")
          .save(FILE_PATH)

        val df2 = spark.read
                    .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
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
          .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .mode("overwrite")
          .save(FILE_PATH)

        val df4 = spark.read
          .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
          .load(FILE_PATH)

        println(df4.count())

    }
}