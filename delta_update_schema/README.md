# DELTA UPDATE SCHEMA

    sbt clean assembly
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/delta_update_schema-assembly-0.1.jar