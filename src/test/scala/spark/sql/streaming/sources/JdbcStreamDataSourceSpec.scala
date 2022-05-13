package spark.sql.streaming.sources

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import utils.{SparkWithHdfsTestingEnv, UnitSpec}

import java.sql.{Connection, Date, DriverManager, Timestamp}
import scala.collection.JavaConverters._

class JdbcStreamDataSourceSpec extends UnitSpec with SparkWithHdfsTestingEnv with DataFrameComparer {

    private lazy val dataDF = sparkSession.createDataFrame(inputData.asJava, dataSchema)
    private val dataSourceFormat = "org.apache.spark.sql.execution.streaming.sources.JdbcStreamingDataSourceProvider"
    private val inputData = Seq(
        Row(1: Byte, 1: Short, 1, 1L, 1.1f, 1.1, BigDecimal(1.111111111111111111111), Timestamp.valueOf("2022-01-01 00:00:00"), Date.valueOf("2022-01-01")),
        Row(2: Byte, 2: Short, 2, 2L, 2.2f, 2.2, BigDecimal(2.222222222222222222222), Timestamp.valueOf("2022-01-01 01:00:00"), Date.valueOf("2022-01-01")),
        Row(3: Byte, 3: Short, 3, 3L, 3.3f, 3.3, BigDecimal(3.333333333333333333333), Timestamp.valueOf("2022-01-02 03:00:00"), Date.valueOf("2022-01-02")),
        Row(4: Byte, 4: Short, 4, 4L, 4.4f, 4.4, BigDecimal(4.444444444444444444444), Timestamp.valueOf("2022-01-05 06:00:00"), Date.valueOf("2022-01-05")),
        Row(5: Byte, 5: Short, 5, 5L, 5.5f, 5.5, BigDecimal(5.555555555555555555555), Timestamp.valueOf("2022-01-05 09:00:00"), Date.valueOf("2022-01-05")),
        Row(6: Byte, 6: Short, 6, 6L, 6.6f, 6.6, BigDecimal(6.666666666666666666666), Timestamp.valueOf("2022-01-06 00:00:00"), Date.valueOf("2022-01-06"))
    )
    private val dataSchema: StructType =
        StructType(List(
            StructField("idByte", ByteType, true),
            StructField("idShort", ShortType, true),
            StructField("idInt", IntegerType, true),
            StructField("idLong", LongType, true),
            StructField("scoreFloat", FloatType, true),
            StructField("scoreDouble", DoubleType, true),
            StructField("scoreDecimal", DecimalType(22, 21), true),
            StructField("time", TimestampType, true),
            StructField("date", DateType, true)
        ))

    private val inputTableName = "input_data"
    private val jdbcDefaultOptions = Map(
        "user" -> "username",
        "password" -> "password",
        "database" -> "h2_db",
        "driver" -> "org.h2.Driver",
        "url" -> "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;DATABASE_TO_UPPER=false",
        "dbtable" -> inputTableName
    )

    override def beforeAll(): Unit = {
        super.beforeAll()
        dataDF.write
          .mode(SaveMode.Overwrite)
          .options(jdbcDefaultOptions)
          .option("createTableColumnTypes", "idByte TINYINT , idShort SMALLINT , idInt INTEGER , idLong BIGINT , scoreFloat REAL , scoreDouble DOUBLE , scoreDecimal DECIMAL(38,21) , time TIMESTAMP , date DATE ")
          .option("dbtable", inputTableName)
          .format("jdbc")
          .save()

    }

    override def afterAll(): Unit = {
        super.afterAll()

        val url = jdbcDefaultOptions("url")
        val driver = jdbcDefaultOptions("driver")
        val username = jdbcDefaultOptions("user")
        val password = jdbcDefaultOptions("password")

        var connection: Connection = null
        try {
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            val statement = connection.createStatement()
            statement.execute("SHUTDOWN")

        } catch {
            case e: Throwable => e.printStackTrace
        }
        finally {
            connection.close()
        }

    }

    private def getDirs(uid: String = java.util.UUID.randomUUID.toString) = {
        ("/checkpoint/" + uid, "/output/" + uid)
    }

    "JDBCStreamSource" should "load all data from table with all types offset column" in {
        dataSchema.fields.foreach {
            field =>
                val (checkpointDir, outputDir) = getDirs()

                val options = jdbcDefaultOptions ++ Map("offsetColumn" -> field.name)
                val writeStream = sparkSession.readStream
                  .format(dataSourceFormat)
                  .options(options)
                  .load()
                  .writeStream
                  .outputMode("append")
                  .format("json")
                  .option("checkpointLocation", checkpointDir)
                  .start(outputDir)

                writeStream.processAllAvailable()
                writeStream.stop()

                val actual = sparkSession.read.schema(dataSchema).json(outputDir)

                assertSmallDatasetEquality(actualDS = actual, expectedDS = dataDF, orderedComparison = false)
        }

    }

    it should "load all data from table with NUMERIC offset column using startingOffset '3'" in {
        dataSchema.fields.filter(_.dataType.isInstanceOf[NumericType]).foreach {
            field => {
                val (checkpointDir, outputDir) = getDirs()

                val options = jdbcDefaultOptions ++ Map("offsetColumn" -> field.name)
                val writeStream = sparkSession.readStream
                  .format(dataSourceFormat)
                  .options(options)
                  .option("startingOffset", "3")
                  .load()
                  .writeStream
                  .outputMode("append")
                  .format("json")
                  .option("checkpointLocation", checkpointDir)
                  .start(outputDir)

                writeStream.processAllAvailable()
                writeStream.stop()

                val actual = sparkSession.read.schema(dataSchema).json(outputDir)
                val expected = dataDF.filter(s"${field.name} >= 3")
                assertSmallDatasetEquality(actualDS = actual, expectedDS = expected, orderedComparison = false)
            }
        }
    }

    it should "load once the data from table with NUMERIC offset column using maxTriggerOffsetRange of 1" in {
        dataSchema.fields.filter(_.dataType.isInstanceOf[NumericType]).foreach {
            field => {
                val (checkpointDir, outputDir) = getDirs()

                val options = jdbcDefaultOptions ++ Map("offsetColumn" -> field.name)
                val writeStream = sparkSession.readStream
                  .format(dataSourceFormat)
                  .options(options)
                  .option("maxTriggerOffsetRange", "1")
                  .load()
                  .writeStream
                  .trigger(Trigger.Once())
                  .outputMode("append")
                  .format("json")
                  .option("checkpointLocation", checkpointDir)
                  .start(outputDir)

                writeStream.processAllAvailable()
                writeStream.stop()

                val actual = sparkSession.read.schema(dataSchema).json(outputDir)
                val expected = dataDF.filter(s"${field.name} <= 2")
                assertSmallDatasetEquality(actualDS = actual, expectedDS = expected, orderedComparison = false)
            }
        }
    }

    it should "load once the data from table with TIME offset column using maxTriggerOffsetRange of 1 day" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "time")
        val writeStream = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "P1DT0H0M0.0S")
          .load()
          .writeStream
          .trigger(Trigger.Once())
          .outputMode("append")
          .format("json")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStream.processAllAvailable()
        writeStream.stop()

        val actual = sparkSession.read.schema(dataSchema).json(outputDir)
        val expected = dataDF.filter("time <= '2022-01-01 01:00:00'")
        assertSmallDatasetEquality(actualDS = actual, expectedDS = expected, orderedComparison = false)
    }

    it should "load once the data from table with DATE offset column using maxTriggerOffsetRange of 1 day" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "date")
        val writeStream = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "P0Y0M1D")
          .load()
          .writeStream
          .trigger(Trigger.Once())
          .outputMode("append")
          .format("json")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStream.processAllAvailable()
        writeStream.stop()

        val actual = sparkSession.read.schema(dataSchema).json(outputDir)
        val expected = dataDF.filter("date <= '2022-01-02'")
        assertSmallDatasetEquality(actualDS = actual, expectedDS = expected, orderedComparison = false)
    }

    it should "load all data from table with INTEGER offset column break writing midway and restore" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "idInt")
        var plannedFail = true

        def runBatch(data: Dataset[Row], id: Long) = {
            if (plannedFail && data.first().getInt(0) == 4)
                throw new RuntimeException("Some unexpected error")
            data.write
              .mode(SaveMode.Append)
              .json(outputDir)
        }


        try {
            val writeStream = sparkSession.readStream
              .format(dataSourceFormat)
              .options(options)
              .option("maxTriggerOffsetRange", "1")
              .load()
              .writeStream
              .outputMode("append")
              .foreachBatch(runBatch _)
              .option("checkpointLocation", checkpointDir)
              .start(outputDir)

            writeStream.processAllAvailable()
            writeStream.stop()
        } catch {
            case ex: Exception =>
                println("Planned error")
        }
        plannedFail = false

        val writeStream = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "1")
          .load()
          .writeStream
          .outputMode("append")
          .foreachBatch(runBatch _)
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStream.processAllAvailable()
        writeStream.stop()

        val actual = sparkSession.read.schema(dataSchema).json(outputDir)
        assertSmallDatasetEquality(actualDS = actual, expectedDS = dataDF, orderedComparison = false)
    }

    it should "load all data from table with TIMESTAMP offset column break writing midway and restore" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "time")
        var plannedFail = true

        def runBatch(data: Dataset[Row], id: Long) = {
            if (plannedFail && data.first().getTimestamp(0).equals(Timestamp.valueOf("2022-01-05 06:00:00")))
                throw new RuntimeException("Some unexpected error")
            data.write
              .mode(SaveMode.Append)
              .json(outputDir)
        }


        try {
            val writeStream = sparkSession.readStream
              .format(dataSourceFormat)
              .options(options)
              .option("maxTriggerOffsetRange", "P1DT0H0M0.0S")
              .load()
              .writeStream
              .outputMode("append")
              .foreachBatch(runBatch _)
              .option("checkpointLocation", checkpointDir)
              .start(outputDir)

            writeStream.processAllAvailable()
            writeStream.stop()
        } catch {
            case ex: Exception =>
                println("Planned error")
        }
        plannedFail = false

        val writeStream = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "P1DT0H0M0.0S")
          .load()
          .writeStream
          .outputMode("append")
          .foreachBatch(runBatch _)
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStream.processAllAvailable()
        writeStream.stop()

        val actual = sparkSession.read.schema(dataSchema).json(outputDir)
        assertSmallDatasetEquality(actualDS = actual, expectedDS = dataDF, orderedComparison = false)
    }

    it should "load all data from table with INTEGER offset column in 2 runs restarting form previous checkpoint" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "idInt")

        val writeStreamOnce = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "1")
          .load()
          .writeStream
          .format("json")
          .trigger(Trigger.Once())
          .outputMode("append")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStreamOnce.awaitTermination()
        writeStreamOnce.stop()

        val writeStreamEnd = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "1")
          .load()
          .writeStream
          .format("json")
          .outputMode("append")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStreamEnd.processAllAvailable()
        writeStreamEnd.stop()

        val actual = sparkSession.read.schema(dataSchema).format("json").load(outputDir)
        assertSmallDatasetEquality(actualDS = actual, expectedDS = dataDF, orderedComparison = false)
    }

    it should "load all data from table with TIMESTAMP offset column in 2 runs restarting form previous checkpoint" in {
        val (checkpointDir, outputDir) = getDirs()

        val options = jdbcDefaultOptions ++ Map("offsetColumn" -> "time")

        val writeStreamOnce = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "P1DT0H0M0.0S")
          .load()
          .writeStream
          .format("json")
          .trigger(Trigger.Once())
          .outputMode("append")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStreamOnce.awaitTermination()
        writeStreamOnce.stop()

        val writeStreamEnd = sparkSession.readStream
          .format(dataSourceFormat)
          .options(options)
          .option("maxTriggerOffsetRange", "P1DT0H0M0.0S")
          .load()
          .writeStream
          .format("json")
          .outputMode("append")
          .option("checkpointLocation", checkpointDir)
          .start(outputDir)

        writeStreamEnd.processAllAvailable()
        writeStreamEnd.stop()

        val actual = sparkSession.read.schema(dataSchema).format("json").load(outputDir)
        assertSmallDatasetEquality(actualDS = actual, expectedDS = dataDF, orderedComparison = false)
    }

}
