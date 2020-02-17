package com.oppo.tagbase.job.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.roaringbitmap.buffer.ImmutableRoaringBitmap

/**
  * Created by daikai on 2020/2/16.
  * 该spark任务功能：构建反向字典数据, 并将数据插入到 hive 表中
  */


object InvertedDictBuildingTask {
  def main(args: Array[String]): Unit = {

    var date = new SimpleDateFormat("yyyyMMdd").format(new Date)

    val appName = "DictBuildJob_" + date + "_task"
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


    // 计算出当前新增的imei
    val rddNew = spark.sql("select distinct" + imeiSrc_colName +
      " from " + imeiSrc_dbName + "." + imeiSrc_tableName +
      " except " +
      "select distinct" + imeiTag_colName +
      " from " + imeiTag_dbName + "." + imeiTag_tableName
    )

    val newlist = rddNew.collect().map(_ (0)).toList

    // 记录当前反向字典最大的id值
    val imeiNum = spark.sql("select max(id) as num" +
      " from " + imeiTag_dbName + "." + imeiTag_tableName
    ).collect()(0).getInt(0)

    // 反向字典下一行 id 应加1
    var startNum = imeiNum + 1

    for (elem <- newlist) {
      println(elem, startNum)
      spark.sql("insert into table " + imeiTag_dbName + "." + imeiTag_tableName +
        " PARTITION(dayno=" + date +
        ") values(" + elem + "," + startNum + ")"
      )
      startNum += 1
    }

  }

}
