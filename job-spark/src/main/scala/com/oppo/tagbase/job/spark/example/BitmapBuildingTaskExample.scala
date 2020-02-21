package com.oppo.tagbase.job.spark.example

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
import scala.collection.JavaConverters._

/**
 * Created by liangjingya on 2020/2/11.
 * 该spark任务功能：读取反向字典hive表和维度hive表，批量生成hfile，本地可执行调试
 */
object BitmapBuildingTaskExample {

  case class eventHiveTable(imei: String, app: String, event: String, version: String, daynum: String)

  case class dictionaryHiveTable(imei: String, id: Long)

  def main(args: Array[String]): Unit = {

    val hiveMeataJson = "{\"hiveDictTable\":{\"dbName\":\"tagbase\",\"tableName\":\"imeiTable\",\"imeiColumnName\":\"imei\",\"idColumnName\":\"id\",\"sliceColumnName\":\"daynum\",\"maxId\":0},\"hiveSrcTable\":{\"dbName\":\"tagbase\",\"tableName\":\"eventTable\",\"dimColumns\":[\"app\",\"event\",\"version\"],\"sliceColumn\":{\"columnName\":\"daynum\",\"columnValue\":\"20200220\"},\"imeiColumnName\":\"imei\"},\"output\":\"D:\\\\workStation\\\\sparkTaskHfile\\\\city_20200211_task\"}";
    val objectMapper = new ObjectMapper
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val hiveMeata = objectMapper.readValue(hiveMeataJson, classOf[HiveMeta])

    val outputPath = hiveMeata.getOutput
    val dbA = hiveMeata.getHiveDictTable.getDbName
    val tableA = hiveMeata.getHiveDictTable.getTableName
    val idColumnA = hiveMeata.getHiveDictTable.getIdColumnName
    val imeiColumnA = hiveMeata.getHiveDictTable.getImeiColumnName
    val dbB = hiveMeata.getHiveSrcTable.getDbName
    val tableB = hiveMeata.getHiveSrcTable.getTableName
    val imeiColumnB = hiveMeata.getHiveSrcTable.getImeiColumnName
    val sliceColumnB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnName
    val sliceValueB = hiveMeata.getHiveSrcTable.getSliceColumn.getColumnValue
    val dimColumnBuilder = new StringBuilder
    hiveMeata.getHiveSrcTable.getDimColumns.asScala.toStream
      .foreach(dimColumnBuilder.append("b.").append(_).append(","))
    val dimColumnB = dimColumnBuilder.deleteCharAt(dimColumnBuilder.size-1).toString()

    val rowkeyDelimiter = "_" //rowkey分隔符
    val familyName = "f1" //hbase的列簇
    val qualifierName = "q1" //hbase的列名
    val appName = dbA + "_" +  tableA + "_bitmap_task" //appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[ImmutableRoaringBitmap]))
    val spark = SparkSession.builder()
      .config(sparkConf)
//      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor
    val familyNameBroadcast = spark.sparkContext.broadcast(familyName)
    val qualifierNameBroadcast = spark.sparkContext.broadcast(qualifierName)
    val rowkeyDelimiterBroadcast = spark.sparkContext.broadcast(rowkeyDelimiter)

    //此处先伪造本地数据模拟，后续从hive表获取
    val eventDS = Seq(
      eventHiveTable("imeia", "wechat", "install", "5.2", "20200220"),
      eventHiveTable("imeib", "qq", "install", "5.1", "20200220"),
      eventHiveTable("imeie", "wechat", "uninstall", "5.0", "20200220"),
      eventHiveTable("imeig", "qq", "install", "5.1", "20200220")
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

    dictionaryDS.createTempView(s"$dbA$tableA")
    eventDS.createTempView(s"$dbB$tableB")

    val data = spark.sql(
      s"""
         |select CONCAT_WS('$rowkeyDelimiter',$dimColumnB) as dimension,
         |a.$idColumnA as index from $dbA$tableA a join $dbB$tableB b
         |on a.$imeiColumnA=b.$imeiColumnB where b.$sliceColumnB=$sliceValueB
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
