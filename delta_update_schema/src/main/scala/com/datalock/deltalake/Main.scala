package com.datalock.deltalake

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main{

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("delta_update_schema")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        val df = spark.read
          .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
          .option("multiline", true)
          .load("data/departements_fr.json")


        // --- création de la table --- //

        df.write
          .format("delta")
          .partitionBy("region_code")
          .saveAsTable("departements")

        spark.sql("ALTER TABLE departements ADD COLUMNS (pays STRING after region_code)")

        spark.table("departements").show()

        spark.sql("ALTER TABLE departements CHANGE COLUMN code AFTER slug;")

        spark.table("departements").show()

        spark.read
            .table("departements")
            .withColumn("code", col("code").cast("integer"))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("departements")

        spark.read
          .table("departements")
          .withColumnRenamed("code", "dep_code")
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable("departements")

        spark.table("departements").show()
    }
}