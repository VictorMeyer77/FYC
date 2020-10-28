# DELTA STREAMING READ WRITE

    sbt clean assembly
    
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/delta_streaming_read_write-assembly-0.1.jar SOURCE
    
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/delta_streaming_read_write-assembly-0.1.jar APPEND
    
    bin/spark-submit --class com.datalock.deltalake.Main target/scala-2.12/delta_streaming_read_write-assembly-0.1.jar COMPLETE