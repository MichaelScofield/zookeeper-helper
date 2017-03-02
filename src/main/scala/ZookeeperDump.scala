import java.util
import java.util.concurrent.TimeUnit

import org.apache.commons.cli.{DefaultParser, HelpFormatter, Options}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.{PathUtils, ZKPaths}
import org.apache.log4j.Logger


/**
  * luofucong at 2016-03-18.
  */
object ZookeeperDump {

  val logger: Logger = Logger.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]) {
    val options = new Options
    options.addOption("src", true, "source Zookeeper server")
    options.addOption("dest", true, "target Zookeeper server")
    options.addOption("fromPath", true, "copy from path")
    options.addOption("toPath", true, "copy to path")

    val parser = new DefaultParser
    val cli = parser.parse(options, args)
    if (!cli.hasOption("src") || !cli.hasOption("dest")
      || !cli.hasOption("fromPath") || !cli.hasOption("toPath")) {
      val helpFormatter = new HelpFormatter
      helpFormatter.printHelp("scala -cp rps-assembly-1.0.jar tool.ZookeeperDump [OPTIONS]", options)
      System.exit(-1)
    }

    val srcZkAddr = cli.getOptionValue("src")
    logger.info("Source Zookeeper server: " + srcZkAddr)
    val targetZkAddr = cli.getOptionValue("dest")
    logger.info("Target Zookeeper server: " + targetZkAddr)
    val fromPath = PathUtils.validatePath(cli.getOptionValue("fromPath"))
    logger.info("Copy from path: " + fromPath)
    val toPath = PathUtils.validatePath(cli.getOptionValue("toPath"))
    logger.info("Copy to path: " + toPath)

    val srcZkClient = createZkClient(srcZkAddr)
    val targetZkClient = if (srcZkAddr == targetZkAddr) srcZkClient else createZkClient(targetZkAddr)
    dump(srcZkClient, targetZkClient, fromPath, toPath)
  }

  private def dump(srcZkClient: CuratorFramework, targetZkClient: CuratorFramework,
                   fromPath: String, toPath: String): Unit = {
    val bytes: Array[Byte] = srcZkClient.getData.forPath(fromPath)
    val fromNodeName = ZKPaths.getNodeFromPath(fromPath)
    val toNodePath = ZKPaths.makePath(toPath, fromNodeName)
    targetZkClient.create().forPath(toNodePath, bytes)
    logger.info("Dump " + fromNodeName + " to " + toNodePath + " value " + new String(bytes))

    val children: util.List[String] = srcZkClient.getChildren.forPath(fromPath)
    if (children != null) {
      for (child <- children) {
        val fromPathNew = ZKPaths.makePath(fromPath, child)
        dump(srcZkClient, targetZkClient, fromPathNew, toNodePath)
      }
    }
  }

  private def createZkClient(addr: String): CuratorFramework = {
    val zkClient = CuratorFrameworkFactory.builder
      .connectString(addr).retryPolicy(new ExponentialBackoffRetry(1000, 10, 60000)).build
    zkClient.start()

    if (!zkClient.blockUntilConnected(8, TimeUnit.SECONDS)) {
      throw new RuntimeException("Cannot connect to Zookeeper: " + addr)
    }
    zkClient
  }
}
