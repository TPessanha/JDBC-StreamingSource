package spark.sql.streaming.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class JdbcStreamingDataSourceProvider extends DataSourceRegister with StreamSourceProvider {

    override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
                              providerName: String, parameters: Map[String, String]): Source = {

        val finalSchema = sourceSchema(sqlContext, schema, providerName, parameters)._2
        new JdbcStreamingDataSource(sqlContext, finalSchema, parameters)
    }

    override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
                              parameters: Map[String, String]): (String, StructType) = {
        (shortName(), schema.getOrElse(getSourceSchema(sqlContext, parameters)))
    }

    override def shortName(): String = "jdbc-streaming"

    private def getSourceSchema(sqlContext: SQLContext, parameters: Map[String, String]): StructType = {
        sqlContext.sparkSession.read
          .format("jdbc")
          .options(parameters)
          .load.schema
    }
}
