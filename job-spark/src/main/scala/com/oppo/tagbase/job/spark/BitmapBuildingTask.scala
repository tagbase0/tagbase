package com.oppo.tagbase.job.spark

import java.io.{ByteArrayOutputStream, DataOutputStream, File}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.oppo.tagbase.job.obj.HiveMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * Created by liangjingya on 2020/2/11.
 * 该spark任务功能：读取反向字典和维度hive表，批量生成hfile
 */
object BitmapBuildingTask {

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
    log.info("hiveMeata: " + hiveMeata)

    val hfileOutputPath = hiveMeata.getOutputPath
    val dictInputPath = hiveMeata.getDictTablePath + "*" + File.separator + "*"
    val db = hiveMeata.getHiveSrcTable.getDbName
    val table = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumn = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumn = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceLeftValue = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueLeft
    val sliceRightValue = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueRight
    val dimColumnBuilder = new StringBuilder
    hiveMeata.getHiveSrcTable.getDimColumns.asScala.toStream
      .foreach(dimColumnBuilder.append("b.").append(_).append(","))
    val dimColumn = dimColumnBuilder.deleteCharAt(dimColumnBuilder.size-1).toString()

    val rowkeyDelimiter = "_" //rowkey分隔符
    val familyName = "f1" //hbase的列簇
    val qualifierName = "q1" //hbase的列名
    val appName = "bitmap_task" //appName命名规范？

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[ImmutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor
    val familyNameBroadcast = spark.sparkContext.broadcast(familyName)
    val qualifierNameBroadcast = spark.sparkContext.broadcast(qualifierName)
    val rowkeyDelimiterBroadcast = spark.sparkContext.broadcast(rowkeyDelimiter)
    val delimiterBroadcast = spark.sparkContext.broadcast(",")

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        invertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDS()
    val dictTable = "dictTable"
    dictDs.createOrReplaceTempView(s"$dictTable")

    val data = spark.sql(
      s"""
         |select CONCAT_WS('$rowkeyDelimiter', $dimColumn) as dimension,
         |a.id as index from $dictTable a join $db.$table b
         |on a.imei=b.$imeiColumn
         |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue
         |""".stripMargin)
      .rdd
      .map(row => (row(0).toString, row(1).toString.toInt))
      .groupByKey()
      .mapPartitions(iter => {
        val rowkeyDelimiterBro = rowkeyDelimiterBroadcast.value
        val shardPrefix = "1" //前期默认1个shard,后续bitmap切分可以在这里处理
        var bitmapList: List[(String, ImmutableRoaringBitmap)] = List()
        while (iter.hasNext) {
          val bitmap = new MutableRoaringBitmap
          val data = iter.next()
          data._2.foreach(bitmap.add(_)) //遍历索引放入roaringbitmap
          val rowkey = shardPrefix + rowkeyDelimiterBro + data._1
          bitmapList :+= (rowkey, bitmap)
        }
        bitmapList.iterator
      })
      .sortByKey(true, 1) //hfile要求rowkey有序
      .map(tuple => {
        val familyName = familyNameBroadcast.value
        val qualifierName = qualifierNameBroadcast.value
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        tuple._2.serialize(dos)
        dos.close()
        val kv = new KeyValue(Bytes.toBytes(tuple._1), Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), bos.toByteArray)
        (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), kv)
      })

    //将数据写hfile到hdfs
    val hadoopConf = new Configuration()
    val hbaseConf = HBaseConfiguration.create(hadoopConf)
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])

    val fileSystem = FileSystem.get(hadoopConf)
    if (fileSystem.exists(new Path(hfileOutputPath))) {
      fileSystem.delete(new Path(hfileOutputPath), true)
    }

    data.saveAsNewAPIHadoopFile(
      hfileOutputPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    spark.stop()

  }

}
