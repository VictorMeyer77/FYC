# SPARK ACID PARQUET

    sbt clean assembly
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/spark_acid_parquet-assembly-0.1.jar