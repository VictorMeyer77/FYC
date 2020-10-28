# DELTA BATCH READ WRITE

    sbt clean assembly
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/delta_batch_read_write-assembly-0.1.jar