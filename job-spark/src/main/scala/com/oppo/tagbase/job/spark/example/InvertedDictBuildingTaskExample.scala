package com.oppo.tagbase.job.spark.example

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

  case class invertedDictHiveTable(imei: String, id: Long, daynum: String)

  def main(args: Array[String]): Unit = {

    val hiveMeataJson = "{\"hiveDictTable\":{\"dbName\":\"default\",\"tableName\":\"dictTable\",\"imeiColumnName\":\"imei\",\"idColumnName\":\"id\",\"sliceColumnName\":\"daynum\",\"maxId\":10},\"hiveSrcTable\":{\"dbName\":\"default\",\"tableName\":\"eventTable\",\"dimColumns\":[\"imei\"],\"sliceColumn\":{\"columnName\":\"daynum\",\"columnValueLeft\":\"20200220\",\"columnValueRight\":\"20200221\"},\"imeiColumnName\":\"imei\"},\"output\":\"20200220\",\"rowCountPath\":\"D:\\\\workStation\\\\sparkTaskHfile\\\\rowCount\"}";
    val objectMapper = new ObjectMapper
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val hiveMeata = objectMapper.readValue(hiveMeataJson, classOf[HiveMeta])

    val partition = hiveMeata.getOutput
    val dbA = hiveMeata.getHiveDictTable.getDbName
    val tableA = hiveMeata.getHiveDictTable.getTableName
    val maxId = hiveMeata.getHiveDictTable.getMaxId
    val imeiColumnA = hiveMeata.getHiveDictTable.getImeiColumnName
    val dbB = hiveMeata.getHiveSrcTable.getDbName
    val tableB = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumnB = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumnB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceLeftValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueLeft
    val sliceRightValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueRight

    val appName = "invertedDict_task_" + partition//appName

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
    val partitionBroadcast = spark.sparkContext.broadcast(partition)

    //此处先伪造本地数据模拟，后续从hive表获取
    val imeiDS = Seq(
      imeiHiveTable("imeix", "20200220"),
      imeiHiveTable("imeiy", "20200220"),
      imeiHiveTable("imeie", "20200220"),
      imeiHiveTable("imeig", "20200220")
    ).toDS()
    val invertedDictDS = Seq(
      invertedDictHiveTable("imeia", 1, "20200220"),
      invertedDictHiveTable("imeib", 2, "20200220"),
      invertedDictHiveTable("imeic", 3, "20200220"),
      invertedDictHiveTable("imeid", 4, "20200220"),
      invertedDictHiveTable("imeie", 5, "20200220"),
      invertedDictHiveTable("imeif", 6, "20200220"),
      invertedDictHiveTable("imeig", 7, "20200220")
    ).toDS()

    invertedDictDS.createTempView(s"$dbA$tableA")
    imeiDS.createTempView(s"$dbB$tableB")

    val data = spark.sql(
      s"""
         |select b.$imeiColumnB from $dbB$tableB b
         |where b.$sliceColumnB>=$sliceLeftValueB and b.$sliceColumnB<$sliceRightValueB
         |""".stripMargin)
        .except(
          spark.sql(
            s"""
               |select a.$imeiColumnA from $dbA$tableA a
               |""".stripMargin)
        )
      .rdd
      .map(imei => imei(0))
      .zipWithIndex()
      .map(imeiMap => {
        val maxId = maxIdBroadcast.value
        val partition = partitionBroadcast.value
        invertedDictHiveTable(imeiMap._1.toString(), maxId + 1 + imeiMap._2, partition)
      })
      .repartition(1)
      .toDS()
      .show()

    spark.stop()

  }

}
