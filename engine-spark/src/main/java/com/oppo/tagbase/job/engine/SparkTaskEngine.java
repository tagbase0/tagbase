package com.oppo.tagbase.job.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.oppo.tagbase.job.engine.exception.JobErrorCode;
import com.oppo.tagbase.job.engine.exception.JobException;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.JobType;
import com.oppo.tagbase.job.engine.obj.TaskMessage;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class SparkTaskEngine extends TaskEngine {

    @Inject
    @Named("bitmapBuildingTaskConfig")
    private SparkTaskConfig bitmapBuildingTaskConfig;

    @Inject
    @Named("invertedDictTaskConfig")
    private SparkTaskConfig invertedDictTaskConfig;

    private Logger log = LoggerFactory.getLogger(SparkTaskEngine.class);

    @Override
    public String submitTask(HiveMeta hiveMeta, JobType type) throws JobException {

        //根据job类型，选择不同的配置提交任务
        ObjectMapper objectMapper=new ObjectMapper();
        String appArgs = null;
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            appArgs = objectMapper.writeValueAsString(hiveMeta);
        } catch (JsonProcessingException e) {
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, e, "parse json error");
        }
        String result = null;
        switch (type){
            case DATA:
                result = submitSparkJob(appArgs, bitmapBuildingTaskConfig);
                break;
            case DICTIONARY:
                result = submitSparkJob(appArgs, invertedDictTaskConfig);
                break;
            default:
                break;
        }
        return result;

    }

    @Override
    public TaskMessage getTaskStatus(String appid, JobType type) throws JobException {

        //向hadoop集群根据appid发送请求获取job执行进度
        String message = null;
        TaskMessage taskMessage = null;
        try {
            switch (type){
                case DATA:
                    message = sendGet(bitmapBuildingTaskConfig.getTrackUrl(), appid);
                    break;
                case DICTIONARY:
                    message = sendGet(invertedDictTaskConfig.getTrackUrl(), appid);
                    break;
                default:
                    break;
            }

            //解析集群返回的信息
            String appJsonKey = "app";
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(message);

            if(rootNode.has(appJsonKey)){
                JsonNode appNode = rootNode.get(appJsonKey);
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                taskMessage = objectMapper.readValue(appNode.toString(), TaskMessage.class);
            }else {
                throw new JobException(JobErrorCode.JOB_MONITOR_ERROR, "job response error, message : %s", message);
            }
        } catch (IOException e) {
            throw new JobException(JobErrorCode.JOB_MONITOR_ERROR, e, "get job status error");
        }
        return taskMessage;
    }

    /*
     SparkLauncher api文档：http://spark.apache.org/docs/2.3.2/api/java/org/apache/spark/launcher/SparkLauncher.html
     windows下模拟测试，需要下载winutil,可以设置环境变量HADOOP_HOME
     https://github.com/amihalik/hadoop-common-2.6.0-bin
     另外下载spark-2.3.2-bin-hadoop2.6.tgz解压，可以设置环境变量SPARK_HOME，提交任务需要本地有客户端
     https://archive.apache.org/dist/spark/spark-2.3.2/
    */
    private String submitSparkJob(String hiveMeta, SparkTaskConfig config) throws JobException {

        String sparkHome = System.getenv("SPARK_HOME");
        if(sparkHome == null){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "submit spark job error, not found SPARK_HOME");
        }
        log.debug("submit Spark Job : {}", hiveMeta);
        log.debug("submit Spark Job : {}", config);

        System.setProperty("user.name", config.getUser());
        SparkAppHandle handle = null;
        String appid = null;
        try {
            handle = new SparkLauncher()
                    .setSparkHome(sparkHome)
                    .setAppResource(config.getJarPath())
                    .setMainClass(config.getMainClass())
                    .setMaster(config.getMaster())
                    .setDeployMode(config.getDeployMode())
                    //这里将hive表等参数传递到job,即main函数的args接收
                    .addAppArgs(hiveMeta)
                    //设置一系列job启动参数
                    .setConf(SparkLauncher.DRIVER_MEMORY, config.getDriverMemory())
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, config.getExecutorMemory())
                    .setConf("spark.executor.instances", config.getExecutorInstances())
                    .setConf(SparkLauncher.EXECUTOR_CORES,config.getExecutorCores())
                    .setConf("spark.default.parallelism", config.getParallelism())
                    .setConf("spark.sql.shuffle.partitions", config.getParallelism())
                    .setConf("spark.yarn.queue", config.getQueue())
                    .setVerbose(true)
                    .startApplication();
        } catch (IOException e) {
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, e, "SparkLauncher submit error");
        }
        //提交任务后轮询appid返回
        while (!handle.getState().isFinal()){
            if(handle.getAppId() != null){
                appid = handle.getAppId();
                break;
            }
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                log.error("thread sleep error", e);
            }
        }
        //TODO 提交失败，多次提交，如果出现这种情况？
        if(appid == null && handle.getState() != SparkAppHandle.State.FINISHED){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "SparkLauncher error, appid is null");
        }
        return appid;

    }

    private String sendGet(String url, String appid) throws IOException {
        String content = null;
        int successCode = 200;
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url + appid);
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == successCode) {
                content = EntityUtils.toString(response.getEntity(), "UTF-8");
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return content;
    }

}
