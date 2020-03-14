package com.oppo.tagbase.job.spark

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.text.SimpleDateFormat
import java.util.Random

import com.fasterxml.jackson.databind.ObjectMapper
import com.oppo.tagbase.job.obj.{DataTaskMeta, FieldType}
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
 * 该spark任务功能：读取反向字典和维度hive表，批量生成action任务的parquet格式文件
 */
object BitmapBuildingActionTask {

  case class InvertedDict(imei: String, tagbaseId: Long)

  case class StringAction(name: String, value: String, metric: Array[Byte], dayno: java.sql.Date, galileo_event_id: Int)

  case class LongAction(name: String, value: Long, metric: Array[Byte], dayno: java.sql.Date, galileo_event_id: Int)

  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    TaskUtil.checkArgs(args)
    val dataMeataJson = args(0)
    log.info("tagbase info, dataMeataJson: " + dataMeataJson)

    val objectMapper = new ObjectMapper
//    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dataTaskMeta = objectMapper.readValue(dataMeataJson, classOf[DataTaskMeta])
    log.info("tagbase info, dataTaskMeta: " + dataTaskMeta)
    TaskUtil.checkPath(dataTaskMeta.getOutputPath)

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
//      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableRoaringBitmap], classOf[MutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

    val parallelism = spark.sparkContext.getConf.get("spark.default.parallelism").toInt
    log.info("spark.default.parallelism: {}", parallelism)

    //driver广播相关参数到executor，用于反向字典的imei和id的分割
    val delimiterBroadcast = spark.sparkContext.broadcast(",")
    val daynoValueBroadcast = spark.sparkContext.broadcast(daynoValue)

    val hadoopConf = new Configuration()
    val fileSystem = FileSystem.get(hadoopConf)
    if (fileSystem.exists(new Path(baseOutputPath))) {
      fileSystem.delete(new Path(baseOutputPath), true)
    }

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        InvertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDS()
    val dictTable = "dictTable"
    dictDs.createOrReplaceTempView(s"$dictTable")

    //galileo.event_all_orc的imei=''会造成数据倾斜
    //TODO 为什么线上join时快时慢，没完全用完资源？
    val sqlStr =
      s"""
         |select $dimColumn  b.$eventIdColumn as tagbaseEventId, a.tagbaseId from $dictTable a join $db.$table b
         |on (b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |and b.$imeiColumn !='' and b.$imeiColumn is not null and b.$eventIdColumn is not null and a.imei=b.$imeiColumn)
         |""".stripMargin
    log.info("tagbase info, sqlStr： {}", sqlStr)


    val flatMapRdd = spark.sql(sqlStr)
      .rdd
      .flatMap(row => {
        var bitmapList: List[((FieldType, String, String, Int), Int)] = List()
        val actionFieldSeq = row.schema.filter(field => !"tagbaseId".equals(field.name) && !"tagbaseEventId".equals(field.name)).seq
        val dimSize = actionFieldSeq.size

        val eventId = row.getInt(dimSize)
        val tagbaseId = row.getLong(dimSize+1).toInt
        for (index <- 0 until dimSize) {
          val fieldName = actionFieldSeq(index).name
          //根据字段类型和null过滤
          if (row(index) != null && !"".equals(row(index).toString)) {
            val fieldValue = row(index).toString
            if (actionFieldSeq(index).dataType == StringType || actionFieldSeq(index).dataType == VarcharType) {
              bitmapList :+= ((FieldType.STRING, fieldName, fieldValue, eventId), tagbaseId)
            } else {
              bitmapList :+= ((FieldType.LONG, fieldName, fieldValue, eventId), tagbaseId)
            }
          }
        }
        bitmapList.iterator
      })

    val bitmapDataRdd = flatMapRdd
      .repartition(parallelism)//flatMap可能不均衡
      .map(row=>{
        val bitmap = MutableRoaringBitmap.bitmapOf(row._2)
        (row._1, bitmap)
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
    log.info(String.format("tagbase info, stringActionRddCount: %s, partitionCount： %s, maxCountPerPartition: %s", stringActionRddCount.toString, stringActionRddPartitionCount.toString, maxCountPerPartition.toString))
    log.info(String.format("tagbase info, longActionRddCount: %s, partitionCount： %s, maxCountPerPartition: %s", longActionRddCount.toString, longActionRddPartitionCount.toString, maxCountPerPartition.toString))

    //写parquet
    stringActionRdd
      .repartition(stringActionRddPartitionCount.toInt)
      .toDS()
      .write.mode(SaveMode.Append)
      .parquet(stringActionOutputPath)

    longActionRdd
      .repartition(longActionRddPartitionCount.toInt)
      .toDS()
      .write.mode(SaveMode.Append)
      .parquet(longActionOutputPath)


    spark.stop()

  }

}


