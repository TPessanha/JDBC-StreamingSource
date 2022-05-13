package spark.sql.streaming.sources

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types._
import spark.sql.streaming.sources.offsets.JdbcOffset

import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}

class JdbcStreamingDataSource(sqlContext: SQLContext,
                              override val schema: StructType,
                              options: Map[String, String] = Map.empty)
  extends Source with LazyLogging {

  private lazy val `type`: DataType = {
    val columnName = sourceOptions.offsetColumn
    val sqlField = schema.fields
      .find(_.name.equalsIgnoreCase(columnName))
      .getOrElse(throw new IllegalArgumentException(s"Column '$columnName' not found in schema."))
    sqlField.dataType
  }

  private val sourceOptions = new JdbcStreamOptions(options)

  private var _startOffset: Option[JdbcOffset] = None
  private var _currentMaxOffset: Option[JdbcOffset] = None

  override def stop(): Unit = {
    /*Do nothing*/
  }

  override def getOffset: Option[Offset] = {
    if (sourceOptions.maxTriggerOffsetRange.isDefined)
      getRangedOffset
    else
      requestMaxOffset
  }

  private def getRangedOffset: Option[JdbcOffset] = {
    if (_startOffset.isEmpty) {
      _startOffset = sourceOptions.startingOffset match {
        case JdbcStreamOptions.STARTING_OFFSET_EARLIEST =>
          requestMinOffset
        case JdbcStreamOptions.STARTING_OFFSET_LATEST =>
          requestMaxOffset
        case custom: String =>
          Some(JdbcOffset(custom, `type`))
      }
    }

    _startOffset match {
      case Some(jdbcOffset) =>
        val cappedOffset = capRangedOffset(jdbcOffset)
        if (_startOffset.get > cappedOffset)
          throw new IllegalStateException(s"Data from source has changed or been deleted. Could not continue stream safely. Reason: 'start:${_startOffset.get.offset} > end:${cappedOffset.offset}'")

        Some(cappedOffset)
      case None => None
    }
  }

  private def capRangedOffset(jdbcOffset: JdbcOffset): JdbcOffset = {
    val newRange = addSubtractRangeToOffset(jdbcOffset)
    if (_currentMaxOffset.isEmpty)
      _currentMaxOffset = requestMaxOffset
    else if (newRange > _currentMaxOffset.get) {
      _currentMaxOffset = requestMaxOffset
    }

    if (_currentMaxOffset.isEmpty)
      throw new IllegalStateException("Data from source has changed or been deleted. Could not continue stream safely. Reason: 'max offset from source is null but previous offset was found'")

    if (newRange <= _currentMaxOffset.get)
      newRange
    else
      _currentMaxOffset.get

  }

  private def addSubtractRangeToOffset(original: JdbcOffset): JdbcOffset = {

    `type` match {
      case DateType =>
        val date = Date.valueOf(original.offset)
        val range = Period.parse(sourceOptions.maxTriggerOffsetRange.get)
        val fullRange = date.toLocalDate.plus(range)
        JdbcOffset(Date.valueOf(fullRange).toString, `type`)

      case TimestampType =>
        val time = Timestamp.valueOf(original.offset)
        val range = Duration.parse(sourceOptions.maxTriggerOffsetRange.get)
        val fullRange = time.toLocalDateTime.plus(range)
        JdbcOffset(Timestamp.valueOf(fullRange).toString, `type`)

      case FloatType | DoubleType =>
        val decimal = original.offset.toDouble
        val range = sourceOptions.maxTriggerOffsetRange.get.toDouble
        JdbcOffset((decimal + range).toString, `type`)

      case _: DecimalType =>
        val decimal = BigDecimal(original.offset)
        val range = BigDecimal(sourceOptions.maxTriggerOffsetRange.get)
        JdbcOffset((decimal + range).toString, `type`)

      case _: NumericType =>
        val number = original.offset.toLong
        val range = sourceOptions.maxTriggerOffsetRange.get.toLong
        JdbcOffset((number + range).toString, `type`)

    }
  }

  private def requestMaxOffset: Option[JdbcOffset] = {
    val dfReader = sqlContext.read
      .format("jdbc")
      .options(sourceOptions.getSparkOptions())
      .option("url", sourceOptions.jdbcOptions.url)

    sourceOptions.user.foreach(dfReader.option("user", _))
    sourceOptions.password.foreach(dfReader.option("password", _))

    val offsetColumn = sourceOptions.offsetColumn
    val query = s"SELECT MAX($offsetColumn) FROM ${sourceOptions.jdbcOptions.tableOrQuery}"
    val maxRow = dfReader
      .option("query", query)
      .load().first()

    if (maxRow.isNullAt(0)) {
      None
    } else {
      Some(JdbcOffset(maxRow.get(0).toString, `type`))
    }
  }

  private def requestMinOffset: Option[JdbcOffset] = {
    val dfReader = sqlContext.read
      .format("jdbc")
      .options(sourceOptions.getSparkOptions())
      .option("url", sourceOptions.jdbcOptions.url)

    sourceOptions.user.foreach(dfReader.option("user", _))
    sourceOptions.password.foreach(dfReader.option("password", _))

    val offsetColumn = sourceOptions.offsetColumn
    val query = s"SELECT MIN($offsetColumn) FROM ${sourceOptions.jdbcOptions.tableOrQuery}"
    val minRow = dfReader
      .option("query", query)
      .load().first()

    if (minRow.isNullAt(0)) {
      None
    } else {
      Some(JdbcOffset(minRow.get(0).toString, `type`))
    }
  }

  /**
   * Returns the data that is between the offsets (`start`, `end`].
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val dfReader = sqlContext.read
      .format("jdbc")
      .schema(schema)
      .options(sourceOptions.getSparkOptions())
      .option("url", sourceOptions.jdbcOptions.url)

    sourceOptions.user.foreach(dfReader.option("user", _))
    sourceOptions.password.foreach(dfReader.option("password", _))

    val rdd = start match {
      case Some(start) =>
        applyBatchRequest(dfReader, Some(JdbcOffset(start)), JdbcOffset(end))
        dfReader.load().queryExecution.toRdd
      case None =>
        sourceOptions.startingOffset match {
          case JdbcStreamOptions.STARTING_OFFSET_LATEST =>
            sqlContext.emptyDataFrame.queryExecution.toRdd
          case _ =>
            applyStartingOffset(dfReader, JdbcOffset(end))
            dfReader.load().queryExecution.toRdd
        }
    }
    val attributes = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
    val plan = LogicalRDD(attributes, rdd.setName("jdbc"), isStreaming = true)(sqlContext.sparkSession)

    val qe = sqlContext.sparkSession.sessionState.executePlan(plan)
    qe.assertAnalyzed()
    val dd = new Dataset[Row](sqlContext.sparkSession, plan, RowEncoder(qe.analyzed.schema))

    //    val df = sqlContext.internalCreateDataFrame(, schema, isStreaming = true)
    val df = dd.toDF()
    _startOffset = Some(JdbcOffset(end))

    df
  }

  private def applyStartingOffset(dfReader: DataFrameReader, endOffset: JdbcOffset): Unit = {
    logger.info(s"No checkpoint was found. Reading from '${sourceOptions.startingOffset}'")

    sourceOptions.startingOffset match {
      case JdbcStreamOptions.STARTING_OFFSET_EARLIEST if sourceOptions.maxTriggerOffsetRange.isEmpty =>
        applyBatchRequest(dfReader, None, endOffset)

      case JdbcStreamOptions.STARTING_OFFSET_EARLIEST if sourceOptions.maxTriggerOffsetRange.isDefined =>
        val min = requestMinOffset
        applyBatchRequest(dfReader, None, addSubtractRangeToOffset(min.get), firstBatch = true)

      case startingOffset =>
        applyBatchRequest(dfReader, Some(JdbcOffset(startingOffset, `type`)), endOffset, firstBatch = true)

    }

  }

  private def applyBatchRequest(dfReader: DataFrameReader, startOffset: Option[JdbcOffset], endOffset: JdbcOffset, firstBatch: Boolean = false): Unit = {
    logger.info(s"Request batch ${if (firstBatch) "[" else "]"}$startOffset, $endOffset]")

    val offsetColumn = sourceOptions.offsetColumn
    val from = sourceOptions.jdbcOptions.tableOrQuery

    val condition = if (firstBatch) ">=" else ">"

    val startCondition =
      if (startOffset.isDefined) s"$offsetColumn $condition CAST('${startOffset.get.offset}' AS ${`type`.sql}) AND "
      else ""

    val query = s"SELECT * FROM $from WHERE " + startCondition + s"$offsetColumn <= CAST('${endOffset.offset}' AS ${`type`.sql})"
    dfReader.option("query", query)
  }

}
