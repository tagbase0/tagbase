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
 * windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
 * https://github.com/amihalik/hadoop-common-2.6.0-bin
 * 另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
 * https://archive.apache.org/dist/spark/spark-2.3.2/
 */
object InvertedDictBuildingTaskExample {

  case class ImeiHiveTable(imei: String, dayno: String)

  case class InvertedDict(imei: String, id: Long)

  def main(args: Array[String]): Unit = {

    val dictMeataJson =
      """
        |{
        |	"dictBasePath": "D:/workStation/tagbase/invertedDict",
        | "maxId":"7",
        |	"maxRowPartition": "80000",
        |	"outputPath": "D:/workStation/tagbase/invertedDict/20200220",
        |	"dbName": "default",
        |	"tableName": "imeiTable",
        |	"imeiColumnName": "imei",
        |	"sliceColumnName": "dayno",
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
//      imeiHiveTable("imeix", "20200220"),
//      imeiHiveTable("imeiy", "20200220"),
//      imeiHiveTable("imeie", "20200220"),
//      imeiHiveTable("imeig", "20200220")
      ImeiHiveTable("imeia", "20200220"),
      ImeiHiveTable("imeib", "20200220"),
      ImeiHiveTable("imeic", "20200220"),
      ImeiHiveTable("imeid", "20200220"),
      ImeiHiveTable("imeie", "20200220"),
      ImeiHiveTable("imeif", "20200220"),
      ImeiHiveTable("imeig", "20200220")
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
          InvertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
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
