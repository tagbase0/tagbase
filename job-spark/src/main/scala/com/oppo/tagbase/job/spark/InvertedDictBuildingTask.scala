package com.oppo.tagbase.job.spark.example

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.oppo.tagbase.job.engine.obj.HiveMeta
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by liangjingya on 2020/2/20.
 * 该spark任务功能：构造反向字典，本地可执行调试
 */

case class invertedDictHiveTable(imei: String, id: Long)

object InvertedDictBuildingTask{

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

    val partition = hiveMeata.getOutput
    val dbA = hiveMeata.getHiveDictTable.getDbName
    val tableA = hiveMeata.getHiveDictTable.getTableName
    val maxId = hiveMeata.getHiveDictTable.getMaxId
    val imeiColumnA = hiveMeata.getHiveDictTable.getImeiColumnName
    val partitionColumnA = hiveMeata.getHiveDictTable.getSliceColumnName
    val dbB = hiveMeata.getHiveSrcTable.getDbName
    val tableB = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumnB = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumnB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValue

    //appName命名规范？
    val appName = "invertedDict_task_" + partition

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

    val newImeiDs = spark.sql(
      s"""
         |select b.$imeiColumnB from $dbB.$tableB b where b.$sliceColumnB=$sliceValueB
         |""".stripMargin)
      .except(
        spark.sql(
          s"""
             |select a.$imeiColumnA from $dbA.$tableA a
             |""".stripMargin)
      )
      .rdd
      .map(imei => imei(0))
      .zipWithIndex()
      .repartition(1)
      .map(imeiMap => {
        val maxId = maxIdBroadcast.value
        invertedDictHiveTable(imeiMap._1.toString(), maxId + 1 + imeiMap._2)
      })
      .toDS()
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitionColumnA)
      .saveAsTable(s"$dbA.$tableA")

    spark.stop()

  }

}
