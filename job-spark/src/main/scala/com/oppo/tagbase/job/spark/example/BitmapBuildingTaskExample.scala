package com.oppo.tagbase.job.spark.example

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.oppo.tagbase.job.obj.DataTaskMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}
import scala.collection.JavaConverters._

/**
 * Created by liangjingya on 2020/2/11.
 * 该spark任务功能：读取反向字典和维度hive表，批量生成hfile，本地可执行调试
 * windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
 * https://github.com/amihalik/hadoop-common-2.6.0-bin
 * 另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
 * https://archive.apache.org/dist/spark/spark-2.3.2/
 */
object BitmapBuildingTaskExample {

  case class EventHiveTable(imei: String, app: String, event: String, version: String, dayno: String)

  case class InvertedDict(imei: String, id: Long)

  def main(args: Array[String]): Unit = {

    val dataMeataJson =
      """
        |{
        |	"dictBasePath": "D:/workStation/tagbase/invertedDict",
        |	"maxRowPartition": "50000000",
        |	"outputPath": "D:/workStation/tagbase/hfileData/jobidxxxx/taskidxxxx",
        |	"dbName": "default",
        |	"tableName": "eventTable",
        |	"dimColumnNames": ["app","event","version"],
        |	"imeiColumnName": "imei",
        |	"sliceColumnName": "dayno",
        |	"sliceColumnnValueLeft": "20200220",
        |	"sliceColumnValueRight": "20200221"
        |}
        |""".stripMargin

    val objectMapper = new ObjectMapper
//   objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val dataTaskMeta = objectMapper.readValue(dataMeataJson, classOf[DataTaskMeta])

    val hfileOutputPath = dataTaskMeta.getOutputPath
    val dictInputPath = dataTaskMeta.getDictBasePath + File.separator + "*"
    val db = dataTaskMeta.getDbName
    val table = dataTaskMeta.getTableName
    val imeiColumn = dataTaskMeta.getImeiColumnName
    val sliceColumn = dataTaskMeta.getSliceColumnName
    val sliceLeftValue = dataTaskMeta.getSliceColumnnValueLeft
    val sliceRightValue = dataTaskMeta.getSliceColumnValueRight
    val dimColumnBuilder = new StringBuilder
    dataTaskMeta.getDimColumnNames.asScala.toStream
      .foreach(dimColumnBuilder.append("b.").append(_).append(","))
    val dimColumn = dimColumnBuilder.deleteCharAt(dimColumnBuilder.size-1).toString()
    val filterColumnBuilder = new StringBuilder
    dataTaskMeta.getDimColumnNames.asScala.toStream
      .foreach(filterColumnBuilder.append(" and b.").append(_).append(" is not NULL "))
    val nullFilter = filterColumnBuilder.toString()

    val rowkeyDelimiter = "\u0001" //rowkey分隔符
    val familyName = "f1" //hbase的列簇
    val qualifierName = "q1" //hbase的列名
    val appName = "tagbase_bitmap_task" //appName

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[4]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "D:\\workStation\\sparkTemp")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[ImmutableRoaringBitmap], classOf[MutableRoaringBitmap], classOf[KeyValue]))
    val spark = SparkSession.builder()
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //driver广播相关参数到executor
    val familyNameBroadcast = spark.sparkContext.broadcast(familyName)
    val qualifierNameBroadcast = spark.sparkContext.broadcast(qualifierName)
    val rowkeyDelimiterBroadcast = spark.sparkContext.broadcast(rowkeyDelimiter)
    val delimiterBroadcast = spark.sparkContext.broadcast(",")

    //此处先伪造本地数据模拟，后续从hive表获取
    val eventDS = Seq(
      EventHiveTable("imeia", "wechat", "install", "5.2", "20200220"),
      EventHiveTable("imeib", "qq", "install", "5.1", "20200220"),
      EventHiveTable("imeie", "wechat", "uninstall", "5.0", "20200220"),
      EventHiveTable("imeig", "qq", "install", "5.1", "20200220")
    ).toDS()
    eventDS.createTempView(s"$db$table")

    val dictDs = spark.sparkContext.textFile(dictInputPath)
      .map(row => {
        val imeiIdMap = row.split(delimiterBroadcast.value)
        InvertedDict(imeiIdMap(0), imeiIdMap(1).toLong)
      })
      .toDS()
    val dictTable = "dictTable"
    dictDs.createOrReplaceTempView(s"$dictTable")


    val bitmapData=spark.sql(
      s"""
         |select CONCAT_WS('$rowkeyDelimiter', $dimColumn) as dimension,
         |a.id as index from $dictTable a join $db$table b
         |on a.imei=b.$imeiColumn
         |where b.$sliceColumn >= $sliceLeftValue and b.$sliceColumn < $sliceRightValue $nullFilter
         |""".stripMargin)
      .rdd
      .map(row => {
        val bitmap = MutableRoaringBitmap.bitmapOf(row(1).toString.toInt)
        (row(0).toString, bitmap)
      })
      .reduceByKey((x,y)=>{
        x.or(y)
        x
      })


    val bitmapCount = bitmapData.count()
    val maxCountPerPartition = 10000
    val partitionCount =
      if (bitmapCount % maxCountPerPartition > 0) (bitmapCount / maxCountPerPartition + 1)
      else (bitmapCount / maxCountPerPartition)

    class bitmapPartitioner() extends Partitioner{
      override def numPartitions: Int = partitionCount.toInt
      override def getPartition(key: Any): Int = {
        val sliceNum = 1//暂时只有一个分片,后续安排切分bitmap后的分区号
        UUID.randomUUID().hashCode() % numPartitions
      }
    }

    implicit val bitmapOrdering = new Ordering[(Int,String)] {
      override def compare(a: (Int,String), b: (Int,String)): Int = {
        a._2.compareTo(b._2)
      }
    }

    val hfileRdd = bitmapData
      .flatMap(kv=>{
        val sliceNum = 1//暂时只有一个分片，后续这里切分bitmap
        var bitmapList: List[((Int,String) ,ImmutableRoaringBitmap)] = List()
        val rowkey = sliceNum + rowkeyDelimiterBroadcast.value + kv._1
        bitmapList :+= ((sliceNum,rowkey), kv._2)
        bitmapList.iterator
      })
      .repartitionAndSortWithinPartitions(new bitmapPartitioner())
      .map(tuple => {
        val key = tuple._1._2
        val value = tuple._2
        val familyName = familyNameBroadcast.value
        val qualifierName = qualifierNameBroadcast.value
        val bos = new ByteArrayOutputStream
        val dos = new DataOutputStream(bos)
        value.serialize(dos)
        dos.close()
        val kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), bos.toByteArray)
        (new ImmutableBytesWritable(Bytes.toBytes(key)), kv)
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
    if (fileSystem.exists(new Path(hfileOutputPath))) {
      fileSystem.delete(new Path(hfileOutputPath), true)
    }

    hfileRdd.saveAsNewAPIHadoopFile(
      hfileOutputPath,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hbaseConf
    )

    spark.stop()

  }

}
