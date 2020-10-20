# SPARK ACID DELTA

    sbt clean assembly
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/spark_acid_delta-assembly-0.1.jar