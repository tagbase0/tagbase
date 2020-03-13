package com.oppo.tagbase.job.spark

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.text.SimpleDateFormat

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
 * 该spark任务功能：读取反向字典和维度hive表，批量生成tag任务的parquet格式文件
 */
object BitmapBuildingTagTask {

  case class InvertedDict(imei: String, tagbaseId: Long)

  case class StringTag(name: String, value: String, metric: Array[Byte], dayno: java.sql.Date)

  case class LongTag(name: String, value: Long, metric: Array[Byte], dayno: java.sql.Date)

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

    val baseOutputPath = dataTaskMeta.getOutputPath + File.separator + "tag"
    val stringTagOutputPath = baseOutputPath + File.separator + "string"
    val longTagOutputPath = baseOutputPath + File.separator + "number"
    val dictInputPath = dataTaskMeta.getDictBasePath + File.separator + "*"
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
    val singleQuotation = "\'"
    val formatter = new SimpleDateFormat(sliceFormat)
    val daynoValue = new java.sql.Date(formatter.parse(sliceLeftValue.replaceAll(singleQuotation, "")).getTime)
    val maxCountPerPartition = if (dataTaskMeta.getMaxRowPartition < 10000) 10000 else dataTaskMeta.getMaxRowPartition

    val appName = "tagbase_tag_task" //appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableRoaringBitmap], classOf[MutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.addResource("core-site.xml")
    spark.sparkContext.hadoopConfiguration.addResource("hdfs-site.xml")

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

    val sqlStr =
      s"""
         |select $dimColumn  a.tagbaseId from $dictTable a join $db.$table b on a.imei=b.$imeiColumn
         |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |""".stripMargin
    log.info("tagbase info, sqlStr： {}", sqlStr)


    //业务处理，组装bitmap
    val bitmapDataRdd = spark.sql(sqlStr)
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
    val stringTagRddPartitionCount = TaskUtil.getPartition(stringTagRddCount, maxCountPerPartition)
    val longTagRddCount = longTagRdd.count()
    val longTagRddPartitionCount = TaskUtil.getPartition(longTagRddCount, maxCountPerPartition)
    log.info(String.format("tagbase info, stringTagRddCount: %s, partitionCount： %s, maxCountPerPartition: %s", stringTagRddCount.toString, stringTagRddPartitionCount.toString, maxCountPerPartition.toString))
    log.info(String.format("tagbase info, longTagRddCount: %s, partitionCount： %s, maxCountPerPartition: %s", longTagRddCount.toString, longTagRddPartitionCount.toString, maxCountPerPartition.toString))

    //写parquet
    stringTagRdd
      .repartition(stringTagRddPartitionCount.toInt)
      .toDS()
      .write.mode(SaveMode.Append)
      .parquet(stringTagOutputPath)

    longTagRdd
      .repartition(longTagRddPartitionCount.toInt)
      .toDS()
      .write.mode(SaveMode.Append)
      .parquet(longTagOutputPath)

    spark.stop()

  }

}


