package com.oppo.tagbase.job.spark

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.oppo.tagbase.job.obj.DictTaskMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by liangjingya on 2020/2/20.
 * 该spark任务功能：构造反向字典
 */

object InvertedDictBuildingTask{

  case class invertedDict(imei: String, id: Long)

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    checkArgs(args)
    val dictMeataJson = args(0)
    log.info("tagbase info, dictMeataJson: {}", dictMeataJson)

    val objectMapper = new ObjectMapper
//    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dictTaskMeta = objectMapper.readValue(dictMeataJson, classOf[DictTaskMeta])
    checkPath(dictTaskMeta.getOutputPath)

    log.info("tagbase info, dictTaskMeta: {}", dictTaskMeta)

    val dictBasePath = dictTaskMeta.getDictBasePath
    val dictInputPath = dictTaskMeta.getDictBasePath + File.separator + "*"
    val dictOutputPath = dictTaskMeta.getOutputPath
    val maxId = dictTaskMeta.getMaxId
    val db = dictTaskMeta.getDbName
    val table = dictTaskMeta.getTableName
    val imeiColumn = dictTaskMeta.getImeiColumnName
    val sliceColumn = dictTaskMeta.getSliceColumnName
    val sliceLeftValue = dictTaskMeta.getSliceColumnnValueLeft
    val sliceRightValue = dictTaskMeta.getSliceColumnValueRight
    val maxCountPerPartition = if (dictTaskMeta.getMaxRowPartition<10000000) 10000000 else dictTaskMeta.getMaxRowPartition

    val appName = "tagbase_invertedDict_task"

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

    //driver广播相关参数到executor
    val maxIdBroadcast = spark.sparkContext.broadcast(maxId)
    val delimiterBroadcast = spark.sparkContext.broadcast(",")

    val fileSystem = FileSystem.get(new Configuration())
    if (fileSystem.exists(new Path(dictOutputPath))) {
      log.info("tagbase info, dictOutputPath is exist, now try to delete, {}", dictOutputPath)
      fileSystem.delete(new Path(dictOutputPath), true)
    }
    val dictPath = new Path(dictBasePath)
    var isFirstBuilding = true
    if (fileSystem.exists(dictPath) && fileSystem.listStatus(dictPath).size>0){
      isFirstBuilding = false
    }

    val schema = StructType(
      Seq(
        StructField("imei", StringType, false),
        StructField("id", LongType, false)
      ))
    val dictDs =
      if (isFirstBuilding) {
        log.info("tagbase info, it is the first time to build invertedDict, {}", dictBasePath)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }
      else spark.sparkContext.textFile(dictInputPath)
        .map(row => {
          val imeiIdMap = row.split(delimiterBroadcast.value)
          invertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
        })
        .toDF()

    val sqlStr =
      s"""
         |select b.$imeiColumn from $db.$table b
         |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |""".stripMargin
    log.info("tagbase info, sqlStr： {}", sqlStr)

    val newImeiRdd = spark.sql(sqlStr)
      .except(dictDs.select("imei"))
      .rdd

    val newImeiCount = newImeiRdd.count()
    val partitionCount =
      if (newImeiCount % maxCountPerPartition > 0) (newImeiCount / maxCountPerPartition + 1)
      else (newImeiCount / maxCountPerPartition)
    log.info(String.format("tagbase info, newImeiCount: %s, partitionCount： %s, maxCountPerPartition: %s", newImeiCount.toString, partitionCount.toString, maxCountPerPartition.toString))
    class imeiIdPartitioner() extends Partitioner{
      override def numPartitions: Int = partitionCount.toInt
      override def getPartition(key: Any): Int = {
//        val k = key.asInstanceOf[Int]
        val k = key.toString.toInt
        k / maxCountPerPartition
      }
    }

    newImeiRdd
      .zipWithIndex()
      .map(imeiMap=>{
        val id = imeiMap._2
        val imei = (imeiMap._1)(0).toString
        (id, imei)
      })
      .repartitionAndSortWithinPartitions(new imeiIdPartitioner())
      .map(dict=>{
        val maxId = maxIdBroadcast.value
        val delimiter = delimiterBroadcast.value
        dict._2  + delimiter + (dict._1+maxId)
      })
      .saveAsTextFile(dictOutputPath)

    spark.stop()

  }


  def checkArgs(args: Array[String]): Unit = {
    if (args == null){
      log.error("tagbase info, illegal parameter, not found args")
      System.exit(1)
    }
    if (args.size > 1){
      log.error("tagbase info, illegal parameter, args size must be 1, {}", args)
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

}

