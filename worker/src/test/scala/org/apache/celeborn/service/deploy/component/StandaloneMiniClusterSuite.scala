package org.apache.celeborn.service.deploy.component

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature

object StandaloneMiniClusterSuite {

  def main(args: Array[String]): Unit = {
    println("hello world!")
    println("args.length = " + args.length)

    val cluster = new StandaloneMiniClusterSuite("/tmp/masterPort.txt")
    cluster.init()
    cluster.close()
  }
}

// accept:
//  - the outputFile of the masterPort
//  - a timeout and a termination fileName
// do:
//  - start the minicluster
//  - output the masterPort to file
//  - loop until timeout, or until the termination file is created
class StandaloneMiniClusterSuite(val masterPortOutputFile: String) extends Logging with MiniClusterFeature {
  var masterPort = 0

  def init(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val (m, _) = setupMiniClusterWithRandomPorts()
    masterPort = m.conf.masterPort
  }

  def close(): Unit = {
    logInfo("all test complete, stop Celeborn mini cluster")
    shutdownMiniCluster()
  }



}
