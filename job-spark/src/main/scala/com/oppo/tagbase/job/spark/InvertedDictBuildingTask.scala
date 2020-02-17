package com.oppo.tagbase.job.spark

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

/**
  * Created by daikai on 2020/2/16.
  * 该spark任务功能：构建反向字典,并保存到 hdfs
  */

case class ImeiHiveTable(database: String, table: String, column: String)
case class ImeiTagTable(database: String, table: String, column: String)

object InvertedDictBuildingTask {
  def main(args: Array[String]): Unit = {

    val appName = "DictBuildJob_20200211_task"
    val parallelism = 5 //任务并行度

    val imeiSrc_dbName = "default";
    val imeiSrc_tableName = "imei_list_test";
    val imeiSrc_colName = "imei";
    val imeiTag_dbName = "default";
    val imeiTag_tableName = "imei_tag_test";
    val imeiTag_colName = "imei";



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


    // 计算出当前新增的imei
    val rddNew = spark.sql("select distinct" + imeiSrc_colName +
      " from " + imeiSrc_dbName + "." + imeiSrc_tableName +
      " except " +
      "select distinct" + imeiTag_colName +
      " from " + imeiTag_dbName + "." + imeiTag_tableName
      ).rdd

    // 记录当前反向字典最大的id值
    val rddImeiNum = spark.sql("select max(id) " +
      " from " + imeiTag_dbName + "." + imeiTag_tableName
    ).rdd.foreach(x => x)


  }

}
