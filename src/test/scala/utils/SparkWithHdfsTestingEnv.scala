package utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Suite

import java.io.File

/**
 * Testing utility for Spark with HDFS and Hive integration.
 * This utility is intended to be mixed in test suites. After each test you may call the clear* methods relevant
 * for your scenario. Restarting any of the components on each test is too slow so the goal is to that only after
 * running a whole test suite.
 */
trait SparkWithHdfsTestingEnv extends HdfsTestingEnv with LazyLogging {
  self: Suite =>

  @transient private lazy val conf = {
    new SparkConf(false)
      .set("spark.ui.enabled", "false")
  }
  @transient private var _ss: SparkSession = _

  final def sparkSession: SparkSession = _ss

  final def sparkContext: SparkContext = _ss.sparkContext

  final def sqlContext: SQLContext = _ss.sqlContext

  def extraSparkConfig(): Map[String, String] = Map.empty

  def clearAll(): Unit = {
    clearSparkCaches()
    clearHiveTables()
    clearHdfsFiles()
  }

  def clearSparkCaches(): Unit = {
    _ss.sqlContext.clearCache()
    _ss.sparkContext.getPersistentRDDs.foreach(_._2.unpersist(blocking = true))
  }

  def clearHiveTables(): Unit = {
    _ss.catalog.listDatabases().collect().foreach { db =>
      _ss.catalog.listTables(db.name).collect().foreach { table =>
        logger.info(s"Dropping table [db=${table.database}, name=${table.name}].")
        _ss.sql(s"DROP TABLE ${table.database}.${table.name}")
      }

      if (_ss.catalog.listTables(db.name).collect().isEmpty && db.name != "default") {
        logger.info(s"Dropping database [name=${db.name}].")
        _ss.sql(s"DROP DATABASE ${db.name}")
      }
    }
  }

  def clearHdfsFiles(): Unit = {
    hdfsFileSystem.listStatus(new Path(s"$nameNodeURI/")).foreach { status =>
      if (status.isDirectory) {
        logger.info(s"Deleting directory recursively '${status.getPath}'.")
        hdfsFileSystem.delete(status.getPath, true)
      } else {
        logger.info(s"Deleting file '${status.getPath}'.")
        hdfsFileSystem.delete(status.getPath, false)
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    _ss = SparkSession.builder
      .config(conf.set("spark.hadoop.fs.defaultFS", nameNodeURI))
      .appName("test")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    _ss.stop()
    _ss = null
    val metastoreDir = new File("metastore_db")
    val warehouseDir = new File("spark-warehouse")
    if (metastoreDir.exists()) JavaUtils.deleteRecursively(metastoreDir)
    if (warehouseDir.exists()) JavaUtils.deleteRecursively(warehouseDir)
    super.afterAll()
  }
}
