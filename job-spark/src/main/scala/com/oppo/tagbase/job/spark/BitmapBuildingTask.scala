package com.oppo.tagbase.job.spark

import java.io.{ByteArrayOutputStream, DataOutputStream}
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
 * 该spark任务功能：读取反向字典hive表和维度hive表，批量生成hfile
 */
object BitmapBuildingTask {

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

    val outputPath = hiveMeata.getOutput
    val dbA = hiveMeata.getHiveDictTable.getDbName
    val tableA = hiveMeata.getHiveDictTable.getTableName
    val idColumnA = hiveMeata.getHiveDictTable.getIdColumnName
    val imeiColumnA = hiveMeata.getHiveDictTable.getImeiColumnName
    val dbB = hiveMeata.getHiveSrcTable.getDbName
    val tableB = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumnB = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumnB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceLeftValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueLeft
    val sliceRightValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValueRight
    val dimColumnBuilder = new StringBuilder
    hiveMeata.getHiveSrcTable.getDimColumns.asScala.toStream
      .foreach(dimColumnBuilder.append("b.").append(_).append(","))
    val dimColumnB = dimColumnBuilder.deleteCharAt(dimColumnBuilder.size-1).toString()

    val rowkeyDelimiter = "_" //rowkey分隔符
    val familyName = "f1" //hbase的列簇
    val qualifierName = "q1" //hbase的列名
    val appName = dbA + "_" +  tableA + "_bitmap_task" //appName命名规范？

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

    val data = spark.sql(
      s"""
         |select CONCAT_WS('$rowkeyDelimiter',$dimColumnB) as dimension,
         |a.$idColumnA as index from $dbA.$tableA a join $dbB.$tableB b
         |on a.$imeiColumnA=b.$imeiColumnB
         |where b.$sliceColumnB>=$sliceLeftValueB and b.$sliceColumnB<$sliceRightValueB
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
    if (fileSystem.exists(new Path(outputPath))) {
      fileSystem.delete(new Path(outputPath), true)
    }

    data.saveAsNewAPIHadoopFile(
      outputPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    spark.stop()

  }

}
