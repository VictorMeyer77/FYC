package com.datalock.deltalake

import org.apache.spark.sql._
import io.delta.tables._
import org.apache.spark.sql.functions._

object Main{

    val DEPARTEMENTS_PATH = "/tmp/spark/delta_del_upd_ups/departements"

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("delta_streaming")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        val df = spark.read
          .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
          .option("multiline", true)
          .load("data/departements_fr.json")

        df.write
          .format("delta")
          .partitionBy("region_code")
          .save(DEPARTEMENTS_PATH)

        val deltaTable = DeltaTable.forPath(spark, DEPARTEMENTS_PATH)

        val dfUpdate = spark.read
          .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
          .option("multiline", true)
          .load("data/departements_fr_2.json")

        deltaTable.as("departements")
          .merge(dfUpdate.as("departements_update"),
              "departements.id = departements_update.id")
          .whenMatched.updateExpr(Map("code" -> "departements_update.code"))
          .whenNotMatched
          .insertAll()
          .execute()

        spark.read
          .format("delta")
          .load(DEPARTEMENTS_PATH)
          .where("region_code == 'COM'")
          .show()

        spark.read
          .format("delta")
          .load(DEPARTEMENTS_PATH)
          .where("region_code == '32'")
          .show()

    }
}