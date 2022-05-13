package spark.sql.streaming.sources.exceptions

final case class MissingRequiredParametersException(private val message: String,
                                                    private val cause: Throwable = None.orNull) extends Exception(message, cause)
