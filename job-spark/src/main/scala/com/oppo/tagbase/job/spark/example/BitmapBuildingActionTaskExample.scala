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

import scala.collection.JavaConverters._

/**
 * Created by liangjingya on 2020/3/12.
 * 该spark任务功能：读取反向字典和维度hive表，批量生成action任务的parquet格式文件，本地可执行调试
 * windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
 * https://github.com/amihalik/hadoop-common-2.6.0-bin
 * 另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
 * https://archive.apache.org/dist/spark/spark-2.3.2/
 */

object BitmapBuildingActionTaskExample {

  case class ActionHiveTable(imei: String, event1: String, event2: String, event3: Int, dayno: Long, galileo_event_id: Int)

  case class InvertedDict(imei: String, tagbaseId: Long)

  case class StringAction(name: String, value: String, metric: Array[Byte], dayno: java.sql.Date, galileo_event_id: Int)

  case class LongAction(name: String, value: Long, metric: Array[Byte], dayno: java.sql.Date, galileo_event_id: Int)

  def main(args: Array[String]): Unit = {

    val dataMeataJson =
      """
        |{
        |	"dictBasePath": "D:/workStation/tagbase/invertedDict",
        |	"maxRowPartition": "10000",
        |	"outputPath": "D:/workStation/tagbase/bitmap/20200313",
        |	"dbName": "default",
        |	"tableName": "eventTable",
        |	"dimColumnNames": ["event1","event2","event3"],
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

    val baseOutputPath = dataTaskMeta.getOutputPath + TaskUtil.fileSeparator + "action"
    val stringActionOutputPath = baseOutputPath + TaskUtil.fileSeparator + "string"
    val longActionOutputPath = baseOutputPath + TaskUtil.fileSeparator + "number"
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
    val eventIdColumn = TaskUtil.eventIdColumn

    val formatter = new SimpleDateFormat(sliceFormat)
    val daynoValue = new java.sql.Date(formatter.parse(sliceLeftValue.replaceAll(TaskUtil.singleQuotation, "")).getTime)
    val maxCountPerPartition = if (dataTaskMeta.getMaxRowPartition < 10000) 10000 else dataTaskMeta.getMaxRowPartition

    val appName = "tagbase_action_task" //appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")
      //.set("spark.sql.crossJoin.enabled", "true")
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
      ActionHiveTable("imeia", "a1", "b1", 50, 20200220, 4),
      ActionHiveTable("imeib", "a2", "b2", 60, 20200220, 4),
      ActionHiveTable("imeic", "a3", "b3", 20, 20200220, 3),
      ActionHiveTable("imeid", "a4", "b4", 20, 20200220, 1)
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
         |select $dimColumn  b.$eventIdColumn as tagbaseEventId, a.tagbaseId from $dictTable a join $db$table b
         |on (a.imei=b.$imeiColumn and b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |and b.$imeiColumn !='' and b.$imeiColumn is not null and b.$eventIdColumn is not null)
         |""".stripMargin)

    //业务处理，组装bitmap
    val bitmapDataRdd = hiveDataDF
      .rdd
      .flatMap(row => {
        var bitmapList: List[((FieldType, String, String, Int), MutableRoaringBitmap)] = List()
        val actionFieldSeq = row.schema.filter(field => !"tagbaseId".equals(field.name) && !"tagbaseEventId".equals(field.name)).seq
        val dimSize = actionFieldSeq.size

        val eventId = row.getInt(dimSize)
        val bitmap = MutableRoaringBitmap.bitmapOf(row.getLong(dimSize+1).toInt)
        for (index <- 0 until dimSize) {
          val fieldName = actionFieldSeq(index).name
          //根据字段类型和null过滤
          if (row(index) != null) {
            val fieldValue = row(index).toString
            if (actionFieldSeq(index).dataType == StringType || actionFieldSeq(index).dataType == VarcharType) {
              bitmapList :+= ((FieldType.STRING, fieldName, fieldValue, eventId), bitmap)
            } else {
              bitmapList :+= ((FieldType.LONG, fieldName, fieldValue, eventId), bitmap)
            }
          }
        }

        bitmapList.iterator
      })
      .reduceByKey((bitmap1, bitmap2) => {
        bitmap1.or(bitmap2)
        bitmap1
      })

    //业务处理，根据action类型拆分
    val stringActionRdd = bitmapDataRdd.filter(_._1._1 == FieldType.STRING)
      .map(row => {
        val daynoValue = daynoValueBroadcast.value
        val bitmap = row._2
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        bitmap.serialize(dos)
        dos.close()
        StringAction(row._1._2.toString, row._1._3.toString, bos.toByteArray, daynoValue, row._1._4)
      })

    val longActionRdd = bitmapDataRdd.filter(_._1._1 == FieldType.LONG)
      .map(row => {
        val daynoValue = daynoValueBroadcast.value
        val bitmap = row._2
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        bitmap.serialize(dos)
        dos.close()
        LongAction(row._1._2.toString, row._1._3.toString.toLong, bos.toByteArray, daynoValue, row._1._4)
      })

    //业务处理，获取分区信息
    val stringActionRddCount = stringActionRdd.count()
    val stringActionRddPartitionCount = TaskUtil.getPartition(stringActionRddCount, maxCountPerPartition)
    val longActionRddCount = longActionRdd.count()
    val longActionRddPartitionCount = TaskUtil.getPartition(longActionRddCount, maxCountPerPartition)

    //写parquet
    stringActionRdd
      .repartition(stringActionRddPartitionCount.toInt)
      .toDS()
      .show()
//      .write.mode(SaveMode.Append)
//      .parquet(stringActionOutputPath)

    longActionRdd
      .repartition(longActionRddPartitionCount.toInt)
      .toDS()
      .show()
//      .write.mode(SaveMode.Append)
//      .parquet(longActionOutputPath)

    spark.stop()
  }

}
