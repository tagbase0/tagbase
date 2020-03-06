package com.oppo.tagbase.job.spark.example

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.oppo.tagbase.job.obj.DictTaskMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by liangjingya on 2020/2/20.
 * 该spark任务功能：构造反向字典，本地可执行调试
 */
object InvertedDictBuildingTaskExample {

  case class imeiHiveTable(imei: String, daynum: String)

  case class invertedDict(imei: String, id: Long)

  def main(args: Array[String]): Unit = {

    val dictMeataJson =
      """
        |{
        |	"dictBasePath": "D:\\workStation\\tagbase\\invertedDict",
        | "maxId":"7",
        |	"maxRowPartition": "80000",
        |	"outputPath": "D:\\workStation\\tagbase\\invertedDict\\20200220",
        |	"dbName": "default",
        |	"tableName": "imeiTable",
        |	"imeiColumnName": "imei",
        |	"sliceColumnName": "daynum",
        |	"sliceColumnnValueLeft": "20200220",
        |	"sliceColumnValueRight": "20200221"
        |}
        |""".stripMargin

    val objectMapper = new ObjectMapper
//    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dictTaskMeta = objectMapper.readValue(dictMeataJson, classOf[DictTaskMeta])

    val maxCountPerPartition = dictTaskMeta.getMaxRowPartition
    val dictBasePath = dictTaskMeta.getDictBasePath
    val dictInputPath = dictTaskMeta.getDictBasePath  + File.separator + "*"
    val dictOutputPath = dictTaskMeta.getOutputPath
    val maxId = dictTaskMeta.getMaxId
    val db = dictTaskMeta.getDbName
    val table = dictTaskMeta.getTableName
    val imeiColumn = dictTaskMeta.getImeiColumnName
    val sliceColumn = dictTaskMeta.getSliceColumnName
    val sliceLeftValue = dictTaskMeta.getSliceColumnnValueLeft
    val sliceRightValue = dictTaskMeta.getSliceColumnValueRight

    val appName = "tagbase_invertedDict_task"//appName

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
//      imeiHiveTable("imeia", "20200220"),
//      imeiHiveTable("imeib", "20200220"),
//      imeiHiveTable("imeic", "20200220"),
//      imeiHiveTable("imeid", "20200220"),
//      imeiHiveTable("imeie", "20200220"),
//      imeiHiveTable("imeif", "20200220"),
//      imeiHiveTable("imeig", "20200220")
    ).toDS()
    imeiDS.createTempView(s"$db$table")

    val fileSystem = FileSystem.get(new Configuration())
    if (fileSystem.exists(new Path(dictOutputPath))) {
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
      if (isFirstBuilding)
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      else spark.sparkContext.textFile(dictInputPath)
        .map(row => {
          val imeiIdMap = row.split(delimiterBroadcast.value)
          invertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
        })
        .toDF()

    val newImeiRdd =
        spark.sql(
          s"""
             |select b.$imeiColumn from $db$table b
             |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
             |""".stripMargin)
          .except(dictDs.select("imei"))
        .rdd

    val newImeiCount = newImeiRdd.count()
    val partitionCount =
      if (newImeiCount % maxCountPerPartition > 0) (newImeiCount / maxCountPerPartition + 1)
      else (newImeiCount / maxCountPerPartition)
    class imeiIdPartitioner() extends Partitioner{
      override def numPartitions: Int = partitionCount.toInt
      override def getPartition(key: Any): Int = {
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

}
