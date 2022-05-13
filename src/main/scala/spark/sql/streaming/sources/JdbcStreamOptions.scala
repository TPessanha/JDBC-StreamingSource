package spark.sql.streaming.sources

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import spark.sql.streaming.sources.exceptions.MissingRequiredParametersException

class JdbcStreamOptions(parameters: CaseInsensitiveMap[String]) {

  /*
   * JDBC options
   */
  val jdbcOptions = new JDBCOptions(parameters)
  /**
   * The user used to authenticate the connection
   */
  val user: Option[String] = parameters.get("user")
  /**
   * The password used to authenticate the connection
   */
  val password: Option[String] = parameters.get("password")
  /**
   * Maximum range to bring in each trigger
   */
  val maxTriggerOffsetRange: Option[String] = parameters.get("maxTriggerOffsetRange")
  /**
   * An always incrementing column used as the control of the current position in the stream.
   * Example: Offset column
   */
  val offsetColumn: String = parameters
    .getOrElse("offsetColumn",
      throw MissingRequiredParametersException("'offsetColumn' must be specified in streaming source"))
  /**
   * The offset of the first run
   */
  val startingOffset: String = parameters.getOrElse("startingOffset", JdbcStreamOptions.STARTING_OFFSET_EARLIEST)

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * Used to pass user options to the underlying batch JDBC data source.
   * Removed options added by the streaming source.
   *
   * @return A map of options
   */
  def getSparkOptions(): Map[String, String] = parameters -- List("dbtable", "query")

}

object JdbcStreamOptions {
  val STARTING_OFFSET_EARLIEST = "earliest"
  val STARTING_OFFSET_LATEST = "latest"
}