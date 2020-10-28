package com.datalock.deltalake

import org.apache.spark.sql._
import io.delta.tables._

object Main{

    val FILE_PATH = "/tmp/spark/delta_history"

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("delta_history")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        // --- création d'une table sur laquelle on ajoute deux fois deux données --- //

        val df = spark.range(200)
        df.write
          .format("delta")
          .mode("append")
          .save(FILE_PATH)

        val df2 = spark.range(50)
        df2.write
          .format("delta")
          .mode("append")
          .save(FILE_PATH)

        val df3 = spark.read
          .format("delta")
          .load(FILE_PATH)

        println(df3.count())

        // --- affichage l'historique de la table --- //

        val table = DeltaTable.forPath(spark, FILE_PATH)
        val fullHistory = table.history()
        fullHistory.show()

        // --- lecture d'une version selectionnée par sa date de création --- //
        // --- Date à modifier --- //

        val df4 = spark.read
          .format("delta")
          .option("timestampAsOf", "2020-10-20 21:34:36")
          .load(FILE_PATH)

        println(df4.count())

        // --- lecture d'une version selectionnée par son numéro --- //

        val df5 = spark.read
          .format("delta")
          .option("versionAsOf", "0")
          .load(FILE_PATH)

        println(df5.count())

        // --- supprime les fichiers déréférencés --- //

        table.vacuum()

    }
}