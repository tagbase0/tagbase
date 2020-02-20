package com.oppo.tagbase.job.example;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by liangjingya on 2020/2/12.
 * example代码，将spark任务的jar包提交到集群执行，回调获取appid
 */
public class LanuncherExample {
    public static void main(String[] args) throws IOException, InterruptedException {

     /*
      windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
      https://github.com/amihalik/hadoop-common-2.6.0-bin

      另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
      https://archive.apache.org/dist/spark/spark-2.3.2/
     */
        System.setProperty("hadoop.home.dir", "D:\\workStation\\hadoop-common-2.6.0-bin-master");
        String sparkHome = "D:\\workStation\\spark-2.3.2-bin-hadoop2.6";
        //指定需要提交到集群执行的jar包,通过mvn clean package编译后会产生该jar，相关依赖同时被打进去

        String applicationJar = "D:\\workStation\\tagbaseGithup\\tagbase\\job-spark\\target\\job-spark-1.0-jar-with-dependencies.jar";
        String mainClass = "com.oppo.tagbase.job.spark.BitmapBuildingTask";
        //本地调试指定临时目录，官方有个问题windows下spark任务执行完无法删除临时文件，不影响测试
        String sparkLocalDir = "D:\\workStation\\sparkTemp";


        CountDownLatch countDownLatch = new CountDownLatch(1);

        //SparkLauncher api文档：http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/launcher/SparkLauncher.html
        SparkAppHandle handle = new SparkLauncher()
                .setSparkHome(sparkHome)
                .setAppResource(applicationJar)
                .setMainClass(mainClass)
                .setMaster("local[4]")
                .setDeployMode("client")
                .setConf("spark.local.dir", sparkLocalDir)
                .addAppArgs("xxxxx")//这里将hive表等参数传递到执行任务,即main函数的args接收,具体参数待定
//                .setConf("spark.driver.memory", "3g")//设置一系列内存等参数
//                .setConf("spark.executor.memory", "8g")
//                .setConf("spark.executor.instances", "10")
//                .setConf("spark.executor.cores", "2")
//                .setConf("spark.default.parallelism", "20")
//                .setConf("spark.sql.shuffle.partitions", "20")
                .setVerbose(true)
                .startApplication(new SparkAppHandle.Listener() {
                    //Callback for changes in the handle's state
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        if (sparkAppHandle.getState() == SparkAppHandle.State.RUNNING) {
                            //当任务提交成功，记录appid
                            System.out.println("task appid is :" + sparkAppHandle.getAppId());
                        } else if (sparkAppHandle.getState().isFinal()) {
                            System.out.println("task final state :" + sparkAppHandle.getState().toString());
                            //countDown只是用于本地调试，等spark任务执行完，主线程退出
                            countDownLatch.countDown();
                        }
                    }

                    //Callback for changes in any information that is not the handle's state，暂时不清楚用途
                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {

                    }
                });

        countDownLatch.await();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("hook shut down");
                if (handle.getAppId() == null) {
                    handle.kill();
                }
            }
        }));

    }
}
