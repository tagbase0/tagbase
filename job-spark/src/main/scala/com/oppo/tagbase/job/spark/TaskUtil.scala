package com.oppo.tagbase.job.spark

import org.slf4j.{Logger, LoggerFactory}

object TaskUtil {

  val log: Logger = LoggerFactory.getLogger(getClass)

  def checkArgs(args: Array[String]): Unit = {
    if (args == null){
      log.error("tagbase info, illegal parameter, not found args")
      System.exit(1)
    }
    if (args.size > 1){
      log.error("tagbase info, illeal parameter, args size must be 1, {}", args)
      System.exit(1)
    }
  }

  def checkPath(path: String): Unit = {
    val dictTablePath = "hdfs://alg-hdfs/business/datacenter/osql/tagbase/invertedDict"
    val tagbasePath = "hdfs://alg-hdfs/business/datacenter/osql/tagbase"
    val osqlPath = "hdfs://alg-hdfs/business/datacenter/osql"
    if (dictTablePath.equals(path) || tagbasePath.equals(path) || osqlPath.equals(path)){
      log.error("tagbase info, illegal Path, {}", path)
      System.exit(1)
    }
  }

  def getPartition(rddCount: Long, maxCountPerPartition: Int): Int ={
    val partitionCount =
      if(rddCount == 0) 1
      else if (rddCount % maxCountPerPartition > 0) rddCount / maxCountPerPartition + 1
      else rddCount / maxCountPerPartition
    partitionCount.toInt
  }
}
