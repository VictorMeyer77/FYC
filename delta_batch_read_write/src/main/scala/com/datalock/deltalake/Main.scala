package com.datalock.deltalake

import org.apache.spark.sql._

object Main{

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("delta_batch_read_write")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        val df = spark.read
          .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
          .option("multiline", true)
          .load("data/departements_fr.json")

        df.show()

        // --- création de la table --- //

        df.write
          .format("delta")
          .partitionBy("region_code")
          .saveAsTable("departements")

        // --- ajout dans la table --- //

        df.write
          .format("delta")
          .mode("append")
          .saveAsTable("departements")

        // --- remplacement de la table --- //

        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("departements")

        val df2 = spark.read
          .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
          .option("multiline", true)
          .load("data/auvergne-rhone-alpes.json")

        // --- remplacement partiel --- //

        df2.write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", "region_code=84")
          .saveAsTable("departements")

        // --- lecture de la table --- //

        spark.table("departements").show()

        spark.table("departements").filter("region_code==84").show()
    }
}