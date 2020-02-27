package com.oppo.tagbase.job.engine.example;

import org.apache.spark.launcher.SparkLauncher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;

/**
 * Created by liangjingya on 2020/2/12.
 * example代码，将spark任务的jar包提交到集群执行，回调获取appid
 */
public class SparkLauncherExample {
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

        String launcherJson = "{\"name\":\"xiaoming\"}";
        CountDownLatch countDownLatch = new CountDownLatch(1);

        //SparkLauncher api文档：http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/launcher/SparkLauncher.html
        SparkLauncher  handle = new SparkLauncher()
                .setSparkHome(sparkHome)
                .setAppResource(applicationJar)
                .setMainClass(mainClass)
                .setMaster("local[4]")
                .setDeployMode("client")
                .setConf("spark.local.dir", sparkLocalDir)
                .addAppArgs(launcherJson)//这里将hive表等参数传递到执行任务,即main函数的args接收,具体参数待定
//                .setConf("spark.driver.memory", "3g")//设置一系列内存等参数
//                .setConf("spark.executor.memory", "8g")
//                .setConf("spark.executor.instances", "10")
//                .setConf("spark.executor.cores", "2")
//                .setConf("spark.default.parallelism", "20")
//                .setConf("spark.sql.shuffle.partitions", "20")
                .setVerbose(true);

        Process process =handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);


    }

    static class InputStreamReaderRunnable implements Runnable {

        private BufferedReader reader;

        private String name;

        InputStreamReaderRunnable(InputStream is, String name) {
            this.reader = new BufferedReader(new InputStreamReader(is));
            this.name = name;
        }

        public void run() {
            System.out.println("InputStream " + name + ":");
            try {
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(line);
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
