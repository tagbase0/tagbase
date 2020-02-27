package com.oppo.tagbase.job.spark.example

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.oppo.tagbase.job.obj.HiveMeta
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by liangjingya on 2020/2/20.
 * 该spark任务功能：构造反向字典
 */

object InvertedDictBuildingTask{

  case class invertedDict(imei: String, id: Long)

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args == null || args.size > 1){
      log.error("illeal parameter, not found hiveMeataJson")
      System.exit(1)
    }

    val hiveMeataJson = args(0)
    log.info("hiveMeataJson: " + hiveMeataJson)

    val objectMapper = new ObjectMapper
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val hiveMeata = objectMapper.readValue(hiveMeataJson, classOf[HiveMeta])

    val dictInputPath = hiveMeata.getDictTablePath + "*" + File.separator + "*"
    val dictOutputPath = hiveMeata.getDictTablePath + hiveMeata.getOutputPath
    val maxId = hiveMeata.getMaxId
    val db = hiveMeata.getHiveSrcTable.getDbName
    val table = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumn = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumn = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceLeftValue = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueLeft
    val sliceRightValue = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueRight

    //appName命名规范？
    val appName = "invertedDict_task"

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor
    val maxIdBroadcast = spark.sparkContext.broadcast(maxId)
    val delimiterBroadcast = spark.sparkContext.broadcast(",")

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        invertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDS()

    spark.sql(
      s"""
         |select b.$imeiColumn from $db.$table b
         |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |""".stripMargin)
      .except(
        dictDs.select("imei")
      )
      .rdd
      .zipWithIndex()
      .map(imeiMap => {
        val maxId = maxIdBroadcast.value
        val delimiter = delimiterBroadcast.value
        (imeiMap._1)(0)  + delimiter + (maxId + 1 + imeiMap._2)
      })
      .repartition(1)
      .saveAsTextFile(dictOutputPath)

    spark.stop()

  }

}
