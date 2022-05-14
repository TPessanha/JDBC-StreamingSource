package utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.network.util.JavaUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File

trait TemporaryDirEnv extends LazyLogging with BeforeAndAfterAll {
  self: Suite =>

  val baseDir = "temp"

  def getRandomDir(uid: String = java.util.UUID.randomUUID.toString): String = {
    s"./$baseDir/$uid"
  }

  def getNamedDir(name: String): String = {
    s"./$baseDir/$name"
  }

  override def afterAll(): Unit = {
    clearFiles()
    super.afterAll()
  }

  def clearFiles() = {
    val tempDir = new File(s"./$baseDir")
    if (tempDir.exists()) {
      logger.info(s"Deleting directory recursively '${tempDir.getPath}'.")
      JavaUtils.deleteRecursively(tempDir)
    }
  }

}
