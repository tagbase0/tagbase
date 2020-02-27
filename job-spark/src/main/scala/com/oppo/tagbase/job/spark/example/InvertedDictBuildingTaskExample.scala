package com.oppo.tagbase.job.spark.example

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.oppo.tagbase.job.obj.HiveMeta
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by liangjingya on 2020/2/20.
 * 该spark任务功能：构造反向字典，本地可执行调试
 */
object InvertedDictBuildingTaskExample {

  case class imeiHiveTable(imei: String, daynum: String)

  case class invertedDict(imei: String, id: Long)

  def main(args: Array[String]): Unit = {

    val hiveMeataJson =
      """
        |{
        |	"dictTablePath":"D:\\workStation\\tagbase\\invertedDict\\",
        |	"maxId":"10",
        |	"hiveSrcTable":{
        |		"dbName":"default",
        |		"tableName":"imeiTable",
        |		"dimColumns":[],
        |		"sliceColumn":{
        |			"columnName":"daynum",
        |			"columnValueLeft":"20200220",
        |			"columnValueRight":"20200221"
        |		},
        |		"imeiColumnName":"imei"
        |	},
        |	"outputPath":"20200220",
        |	"rowCountPath":""
        |}
        |""".stripMargin;

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

    val appName = "invertedDict_task"//appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor
    val maxIdBroadcast = spark.sparkContext.broadcast(maxId)
    val delimiterBroadcast = spark.sparkContext.broadcast(",")

    //此处先伪造本地数据模拟，后续从hive表获取
    val imeiDS = Seq(
      imeiHiveTable("imeix", "20200220"),
      imeiHiveTable("imeiy", "20200220"),
      imeiHiveTable("imeie", "20200220"),
      imeiHiveTable("imeig", "20200220")
    ).toDS()
    imeiDS.createTempView(s"$db$table")

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        invertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDS()

    spark.sql(
      s"""
         |select b.$imeiColumn from $db$table b
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
