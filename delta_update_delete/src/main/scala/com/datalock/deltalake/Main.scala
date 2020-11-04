package com.datalock.deltalake

import org.apache.spark.sql._
import io.delta.tables._
import org.apache.spark.sql.functions._

object Main{

    val DEPARTEMENTS_PATH= "/tmp/spark/delta_del_upd/departements"

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

        import spark.implicits._

        val deltaTable = DeltaTable.forPath(spark, DEPARTEMENTS_PATH)

        deltaTable.delete("region_code < 10")
        //deltaTable.delete(col("region_code") < 10)

        spark.read.format("delta").load(DEPARTEMENTS_PATH).show()
        spark.read.format("delta").load(DEPARTEMENTS_PATH).where("region_code < 10").show()

        deltaTable.updateExpr("slug = 'polynesie francaise'",
            Map("slug" -> "'polynésie francaise'"))

        //deltaTable.update(col("slug") === "polynesie francaise",
        //                 Map("slug" -> lit("polynésie francaise")))

        spark.read.format("delta").load(DEPARTEMENTS_PATH).where("region_code == 'COM'").show()

    }
}