package com.oppo.tagbase.job.spark

import java.io.{ByteArrayOutputStream, DataOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}


/**
 * Created by liangjingya on 2020/2/11.
 * 该spark任务功能：读取反向字典hive表和维度hive表，批量生成hfile
 */

case class eventHiveTable(imei: String, app: String, event: String, version: String)

case class dictionaryHiveTable(imei: String, index: Int)

object BitmapBuildingTask {
  def main(args: Array[String]): Unit = {

    /*
      windows下模拟测试，需要下载winutil,设置环境变量HADOOP_HOME
      https://github.com/amihalik/hadoop-common-2.6.0-bin
     */
    System.setProperty("hadoop.home.dir", "D:\\workStation\\hadoop-common-2.6.0-bin-master")

    /*
       此处后续接收命令行相关参数,包括hive表元数据，hbase列簇等，具体参数待定，用来设置任务
     */
    val appName = "city_20200211_task" //appName
    val parallelism = 5 //任务并行度
    val rowkeyDelimiter = "_" //rowkey分隔符
    val hFilePath = "D:\\workStation\\sparkTaskHfile\\" + appName //hFile的存储路径
    val familyName = "f1" //hbase的列簇
    val qualifierName = "q1" //hbase的列名
    val tableAHiveImeiColumName = "imei" //hive表的列名
    val tableAHiveDimColumName = "app" //hive表的列名
    val tableAHiveDim2ColumName = "event" //hive表的列名
    val tableAHiveDim3ColumName = "version" //hive表的列名
    val tableBHiveImeiColumName = "imei" //hive表的列名
    val tableBHiveDimColumName = "index" //hive表的列名

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", parallelism.toString)
      .set("spark.sql.shuffle.partitions", parallelism.toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[ImmutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
//      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    /*
      driver广播相关参数到executor
     */
    val familyNameBroadcast = spark.sparkContext.broadcast(familyName)
    val qualifierNameBroadcast = spark.sparkContext.broadcast(qualifierName)
    val rowkeyDelimiterBroadcast = spark.sparkContext.broadcast(rowkeyDelimiter)

    /*
       此处先伪造本地数据模拟，后续从hive表获取
     */
    val eventDS = Seq(
      eventHiveTable("imeia", "wechat", "install", "5.2"),
      eventHiveTable("imeib", "qq", "install", "5.1"),
      eventHiveTable("imeie", "wechat", "uninstall", "5.0"),
      eventHiveTable("imeig", "qq", "install", "5.1")
    ).toDS()
    val dictionaryDS = Seq(
      dictionaryHiveTable("imeia", 1),
      dictionaryHiveTable("imeib", 2),
      dictionaryHiveTable("imeic", 3),
      dictionaryHiveTable("imeid", 4),
      dictionaryHiveTable("imeie", 5),
      dictionaryHiveTable("imeif", 6),
      dictionaryHiveTable("imeig", 7)
    ).toDS()

    /*
       业务处理，开始部分的拼接sql，后续改为根据维度个数用CONCAT_WS动态拼接
     */
    val eventDSTable = "eventTable"
    val dictionaryDSTable = "dictionaryTable"
    eventDS.createOrReplaceTempView(s"$eventDSTable")
    dictionaryDS.createOrReplaceTempView(s"$dictionaryDSTable")

    val data = spark.sql(
      s"""
         |select CONCAT_WS('$rowkeyDelimiter',a.$tableAHiveDimColumName,a.$tableAHiveDim2ColumName,a.$tableAHiveDim3ColumName) as dimension,
         |b.$tableBHiveDimColumName as index from $eventDSTable a join $dictionaryDSTable b
         |on a.$tableAHiveImeiColumName=b.$tableBHiveImeiColumName
         |""".stripMargin)
      .rdd
      .map(row => (row(0).toString, row(1).toString.toInt))
      .groupByKey(parallelism)
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

    /*
       将数据写hfile到hdfs
     */
    val hadoopConf = new Configuration()
    val hbaseConf = HBaseConfiguration.create(hadoopConf)
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])

    val fileSystem = FileSystem.get(hadoopConf)
    if (fileSystem.exists(new Path(hFilePath))) {
      fileSystem.delete(new Path(hFilePath), true)
    }

    data.saveAsNewAPIHadoopFile(
      hFilePath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    spark.stop()

  }

}
