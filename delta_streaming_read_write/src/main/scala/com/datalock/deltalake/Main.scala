package com.datalock.deltalake

import org.apache.spark.sql._

object Main{

    val DEPARTEMENTS_SOURCE_PATH = "/tmp/spark/delta_streaming_read_write/departements"
    val DEPARTEMENTS_CIBLE_PATH = "/tmp/spark/delta_streaming_read_write/streamDepartements"
    val COUNT_DEP_BY_REGION = "/tmp/spark/delta_streaming_read_write/ctDepartementsByRegion"

    def main (args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
          .builder()
          .appName("delta_streaming_read_write")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()

        args(0) match {

            case "SOURCE" => {

                // --- Création table départements --- //

                val df = spark.read
                  .format("org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2")
                  .option("multiline", true)
                  .load("data/departements_fr.json")

                df.write
                  .format("delta")
                  .partitionBy("region_code")
                  .save(DEPARTEMENTS_SOURCE_PATH)

                while(true){

                    // --- ajout toutes les 5 secondes de données dans la table ---//

                    Thread.sleep(5000)

                    df.write
                      .format("delta")
                      .partitionBy("region_code")
                      .mode("append")
                      .save(DEPARTEMENTS_SOURCE_PATH)


                    try{

                        // --- lecture des tables alimentées par les traitements streaming ---//

                        Thread.sleep(5000)

                        println(spark.read
                          .format("delta")
                          .load(DEPARTEMENTS_CIBLE_PATH)
                          .count())

                        spark.read
                          .format("delta")
                          .load(COUNT_DEP_BY_REGION)
                          .show()


                    } catch {
                        case e: Exception => println(e)
                    }
                }
            }
            case "APPEND" => {

                // --- initialisation de la stream de la table départements --- //

                val stream = spark.readStream
                  .format("delta")
                  .load(DEPARTEMENTS_SOURCE_PATH)

                /*
                val streamWithoutDelete = spark.readStream
                                        .format("delta")
                                        // ignore les suppressions de la table source
                                        .option("ignoreDeletes", "true")
                                        // commence la stream à partir de la version 0 de la table
                                        .option("startingVersion", "0")
                                        .load(DEPARTEMENTS_SOURCE_PATH)

                val streamWithoutChanges = spark.readStream
                                        .format("delta")
                                        // re-traite la table si une modification est survenue
                                        .option("ignoreChanges", "true")
                                        // commence la stream à partir de la version de la table à la date du ...
                                        .option("startingTimestamp", "2020-10-28")
                                        .load(DEPARTEMENTS_SOURCE_PATH)
                 */

                println(stream.schema)
                println(stream.isStreaming)

                // --- copie en streaming des données de la table départements dans une autre table --- //

                stream.writeStream
                  .format("delta")
                  .outputMode("append")
                  .option("checkpointLocation", "delta/events/checkpoints/streamDepartements")
                  .start(DEPARTEMENTS_CIBLE_PATH)
                  .awaitTermination()

            }
            case "COMPLETE" => {

                // --- création de la table qui va stocker le comptage --- //

                spark.sql("CREATE TABLE ctDepartementsByRegion (region_code STRING, count LONG) USING DELTA")

                // --- initialisation de la stream de la table départements --- //

                val stream = spark.readStream
                  .format("delta")
                  .load(DEPARTEMENTS_SOURCE_PATH)

                /*
                val streamWithoutDelete = spark.readStream
                                        .format("delta")
                                        .option("ignoreDeletes", "true")
                                        .option("startingVersion", "0")
                                        .load(DEPARTEMENTS_SOURCE_PATH)

                val streamWithoutChanges = spark.readStream
                                        .format("delta")
                                        .option("ignoreChanges", "true")
                                        .option("startingTimestamp", "2020-10-28")
                                        .load(DEPARTEMENTS_SOURCE_PATH)
                 */

                println(stream.schema)
                println(stream.isStreaming)

                // --- mise à jour en streaming d'une table qui stocke le comptage des départements par région --- //

                stream.groupBy("region_code")
                  .count()
                  .writeStream
                  .format("delta")
                  .outputMode("complete")
                  .option("checkpointLocation", "delta/events/checkpoints/ctDepartementsByRegion")
                  .start(COUNT_DEP_BY_REGION)
                  .awaitTermination()

            }
            case _ => println("Please insert one argument. \"SOURCE\", \"APPEND\" or \"COMPLETE\"")
        }
    }
}