package spark.sql.streaming.sources

import org.apache.spark.sql.SparkSession

object ManualTest extends App {
  val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  sparkSession.readStream
    .format("jdbc-streaming")
    .load().explain()
}
