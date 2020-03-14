package com.oppo.tagbase.job.spark.example

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.ObjectMapper
import com.oppo.tagbase.job.obj.{DataTaskMeta, FieldType}
import com.oppo.tagbase.job.spark.TaskUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, VarcharType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * Created by liangjingya on 2020/3/12.
 * 该spark任务功能：读取反向字典和维度hive表，批量生成tag任务的parquet格式文件，本地可执行调试
 * windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
 * https://github.com/amihalik/hadoop-common-2.6.0-bin
 * 另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
 * https://archive.apache.org/dist/spark/spark-2.3.2/
 */
object BitmapBuildingTagTaskExample {

  case class TagHiveTable(imei: String, city: String, age: Int, gender: String, dayno: Long)

  case class InvertedDict(imei: String, tagbaseId: Long)

  case class StringTag(name: String, value: String, metric: Array[Byte], dayno: java.sql.Date)

  case class LongTag(name: String, value: Long, metric: Array[Byte], dayno: java.sql.Date)

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val dataMeataJson =
      """
        |{
        |	"dictBasePath": "D:/workStation/tagbase/invertedDict",
        |	"maxRowPartition": "10000",
        |	"outputPath": "D:/workStation/tagbase/bitmap/20200313",
        |	"dbName": "default",
        |	"tableName": "eventTable",
        |	"dimColumnNames": ["city","age","gender"],
        |	"imeiColumnName": "imei",
        |	"sliceColumnName": "dayno",
        |	"sliceColumnnValueLeft": "20200220",
        |	"sliceColumnValueRight": "20200221",
        | "sliceColumnFormat": "yyyyMMdd"
        |}
        |""".stripMargin

    val objectMapper = new ObjectMapper
    //   objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dataTaskMeta = objectMapper.readValue(dataMeataJson, classOf[DataTaskMeta])

    val baseOutputPath = dataTaskMeta.getOutputPath + TaskUtil.fileSeparator + "tag"
    val stringTagOutputPath = baseOutputPath + TaskUtil.fileSeparator + "string"
    val longTagOutputPath = baseOutputPath + TaskUtil.fileSeparator + "number"
    val dictInputPath = dataTaskMeta.getDictBasePath + TaskUtil.fileSeparator + "*"
    val db = dataTaskMeta.getDbName
    val table = dataTaskMeta.getTableName
    val imeiColumn = dataTaskMeta.getImeiColumnName
    val sliceColumn = dataTaskMeta.getSliceColumnName
    val sliceLeftValue = dataTaskMeta.getSliceColumnnValueLeft
    val sliceRightValue = dataTaskMeta.getSliceColumnValueRight
    val sliceFormat = dataTaskMeta.getSliceColumnFormat
    val dimColumnBuilder = new StringBuilder
    dataTaskMeta.getDimColumnNames.asScala.toStream
      .foreach(dimColumnBuilder.append("b.").append(_).append(","))
    val dimColumn = dimColumnBuilder.toString()

    val formatter = new SimpleDateFormat(sliceFormat)
    val daynoValue = new java.sql.Date(formatter.parse(sliceLeftValue.replaceAll(TaskUtil.singleQuotation, "")).getTime)
    val maxCountPerPartition = if (dataTaskMeta.getMaxRowPartition < 10000) 10000 else dataTaskMeta.getMaxRowPartition

    val appName = "tagbase_tag_task" //appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "D:\\workStation\\sparkTemp")
      .registerKryoClasses(Array(classOf[ImmutableRoaringBitmap], classOf[MutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor，用于反向字典的imei和id的分割
    val delimiterBroadcast = spark.sparkContext.broadcast(",")
    val daynoValueBroadcast = spark.sparkContext.broadcast(daynoValue)

    val hadoopConf = new Configuration()
    val fileSystem = FileSystem.get(hadoopConf)
    if (fileSystem.exists(new Path(baseOutputPath))) {
      fileSystem.delete(new Path(baseOutputPath), true)
    }

    //此处先伪造本地数据模拟，后续从hive表获取
    val eventDS = Seq(
      TagHiveTable("imeia", "beijing", 20, "female", 20200220),
      TagHiveTable("imeib", "shanghai", 30, "female", 20200220),
      TagHiveTable("imeic", "beijing", 40, "male", 20200220),
      TagHiveTable("imeid", "shanghai", 20, "male", 20200220)
    ).toDS()
    eventDS.createTempView(s"$db$table")

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        InvertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDF()
    val dictTable = "dictTable"
    dictDs.createOrReplaceTempView(s"$dictTable")

    val hiveDataDF = spark.sql(
      s"""
         |select $dimColumn  a.tagbaseId from $dictTable a join $db$table b
         |on (a.imei=b.$imeiColumn and b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue)
         |""".stripMargin)

    //业务处理，组装bitmap
    val bitmapDataRdd = hiveDataDF
      .rdd
      .flatMap(row => {
        var bitmapList: List[((FieldType, String, String), MutableRoaringBitmap)] = List()
        val tagFieldSeq = row.schema.filter(field => !"tagbaseId".equals(field.name)).seq
        val dimSize = tagFieldSeq.size
        val bitmap = MutableRoaringBitmap.bitmapOf(row.getLong(dimSize).toInt)
        for (index <- 0 until dimSize) {
          val fieldName = tagFieldSeq(index).name
          //根据字段类型和null过滤
          if (row(index) != null) {
            val fieldValue = row(index).toString
            if (tagFieldSeq(index).dataType == StringType || tagFieldSeq(index).dataType == VarcharType) {
              bitmapList :+= ((FieldType.STRING, fieldName, fieldValue), bitmap)
            } else {
              bitmapList :+= ((FieldType.LONG, fieldName, fieldValue), bitmap)
            }
          }
        }
        bitmapList.iterator
      })
      .reduceByKey((bitmap1, bitmap2) => {
        bitmap1.or(bitmap2)
        bitmap1
      })

    //业务处理，根据tag类型拆分
    val stringTagRdd = bitmapDataRdd.filter(_._1._1 == FieldType.STRING)
      .map(row => {
        val daynoValue = daynoValueBroadcast.value
        val bitmap = row._2
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        bitmap.serialize(dos)
        dos.close()
        StringTag(row._1._2.toString, row._1._3.toString, bos.toByteArray, daynoValue)
      })

    val longTagRdd = bitmapDataRdd.filter(_._1._1 == FieldType.LONG)
      .map(row => {
        val daynoValue = daynoValueBroadcast.value
        val bitmap = row._2
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        bitmap.serialize(dos)
        dos.close()
        LongTag(row._1._2.toString, row._1._3.toString.toLong, bos.toByteArray, daynoValue)
      })

    //业务处理，获取分区信息
    val stringTagRddCount = stringTagRdd.count()
    val stringTagRddPartitionCount = getPartition(stringTagRddCount, maxCountPerPartition)
    val longTagRddCount = longTagRdd.count()
    val longTagRddPartitionCount = getPartition(longTagRddCount, maxCountPerPartition)

    //写parquet
    stringTagRdd
      .repartition(stringTagRddPartitionCount.toInt)
      .toDS()
      .show()
//      .write.mode(SaveMode.Append)
//      .parquet(stringTagOutputPath)

    longTagRdd
      .repartition(longTagRddPartitionCount.toInt)
      .toDS()
      .show()
//      .write.mode(SaveMode.Append)
//      .parquet(longTagOutputPath)

    spark.stop()
  }

  def getPartition(rddCount: Long, maxCountPerPartition: Int): Int ={
    val partitionCount =
      if(rddCount == 0) 1
      else if (rddCount % maxCountPerPartition > 0) rddCount / maxCountPerPartition + 1
      else rddCount / maxCountPerPartition
    partitionCount.toInt
  }
}
