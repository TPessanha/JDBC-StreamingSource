package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File

trait HdfsTestingEnv extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _hdfsCluster: MiniDFSCluster = _

  def nameNodeURI: String = _hdfsCluster.getURI.toString

  def hdfsConfiguration: Configuration = hdfsFileSystem.getConf

  def hdfsFileSystem: FileSystem = _hdfsCluster.getFileSystem

  def restartAndClearHdfs(): Unit = {
    stop()
    start()
  }

  private def start(): Unit = {
    val baseDir = new File(PathUtils.getTestDir(getClass), "miniHDFS")
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    conf.setBoolean("dfs.webhdfs.enabled", true)

    val hdfsCluster = new MiniDFSCluster.Builder(conf)
      .nameNodePort(9000)
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()
    hdfsCluster.waitClusterUp()
    _hdfsCluster = hdfsCluster
  }

  private def stop(): Unit = {
    _hdfsCluster.shutdown(true)
    _hdfsCluster = null
  }

  override def beforeAll(): Unit = {
    start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    stop()
    super.afterAll()
  }
}
