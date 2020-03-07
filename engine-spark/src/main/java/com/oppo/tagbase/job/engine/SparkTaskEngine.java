package com.oppo.tagbase.job.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.oppo.tagbase.common.guice.ExtensionImpl;
import com.oppo.tagbase.job.engine.obj.DataTaskMeta;
import com.oppo.tagbase.job.engine.obj.DictTaskMeta;
import com.oppo.tagbase.jobv2.JobErrorCode;
import com.oppo.tagbase.jobv2.JobException;
import com.oppo.tagbase.jobv2.spi.DataTaskContext;
import com.oppo.tagbase.jobv2.spi.DictTaskContext;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Props;
import com.oppo.tagbase.meta.obj.ResourceColType;
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
import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by liangjingya on 2020/2/20.
 */

@ExtensionImpl(name = "spark", extensionPoint = TaskEngine.class)
public class SparkTaskEngine implements TaskEngine {

    @Inject
    private SparkTaskConfig defaultTaskConfig;

    private Logger log = LoggerFactory.getLogger(SparkTaskEngine.class);

    private static final String SINGLE_QUOTATION = "\'";

    @Override
    public String buildDict(DictTaskContext context) throws JobException {

        DictTaskMeta taskMeta = getDictTaskMeta(context);
        Map<String,String> taskConfigMap = getTaskConfig(context.getJobProps());
        ObjectMapper objectMapper=new ObjectMapper();
        String appArgs = null;
        try {
//            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            appArgs = objectMapper.writeValueAsString(taskMeta);
        } catch (JsonProcessingException e) {
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, e, "parse json error");
        }
        String mainClass = "com.oppo.tagbase.job.spark.InvertedDictBuildingTask";
//        String mainClass = "com.oppo.tagbase.job.spark.example.InvertedDictBuildingTaskExample";
        String result = submitSparkJob(appArgs, taskConfigMap, mainClass);

        return result;
    }

    private DictTaskMeta getDictTaskMeta(DictTaskContext context) {

        DictTaskMeta taskMeta = new DictTaskMeta();
        taskMeta.setDbName(context.getDictHiveInputConfig().getDbName());
        taskMeta.setTableName(context.getDictHiveInputConfig().getTableName());
        taskMeta.setImeiColumnName(context.getDictHiveInputConfig().getColumn());
        taskMeta.setSliceColumnName(context.getDictHiveInputConfig().getPartitionColumn());

        DateTimeFormatter df = DateTimeFormatter.ofPattern(context.getDictHiveInputConfig().getPartitionColumnFormat().getFormat());
        if(context.getDictHiveInputConfig().getPartitionColumnType() == ResourceColType.STRING) {
            taskMeta.setSliceColumnnValueLeft(SINGLE_QUOTATION + df.format(context.getLowerBound()) + SINGLE_QUOTATION);
            taskMeta.setSliceColumnValueRight(SINGLE_QUOTATION + df.format(context.getUpperBound()) + SINGLE_QUOTATION);
        }else{
            taskMeta.setSliceColumnnValueLeft(df.format(context.getLowerBound()));
            taskMeta.setSliceColumnValueRight(df.format(context.getUpperBound()));
        }

        taskMeta.setMaxId(context.getNextId());
        taskMeta.setDictBasePath(context.getInvertedDictPath());
        taskMeta.setOutputPath(context.getOutputLocation());

        taskMeta.setMaxRowPartition(defaultTaskConfig.getShardItems());//设置默认文件切分限制
        context.getJobProps().stream()
                .filter(props -> Props.KEY_SHARD_ITEMS.equals(props.getKey()))
                .forEach(props -> taskMeta.setMaxRowPartition(Integer.parseInt(props.getValue())));

        return taskMeta;
    }

    @Override
    public String buildData(DataTaskContext context) throws JobException {

        DataTaskMeta taskMeta = getDataTaskMeta(context);
        Map<String,String> taskConfigMap = getTaskConfig(context.getJobProps());
        ObjectMapper objectMapper=new ObjectMapper();
        String appArgs = null;
        try {
//            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            appArgs = objectMapper.writeValueAsString(taskMeta);
        } catch (JsonProcessingException e) {
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, e, "parse json error");
        }
        String mainClass = "com.oppo.tagbase.job.spark.BitmapBuildingTask";
//        String mainClass = "com.oppo.tagbase.job.spark.example.BitmapBuildingTaskExample";
        String result = submitSparkJob(appArgs, taskConfigMap, mainClass);

        return result;
    }

    private DataTaskMeta getDataTaskMeta(DataTaskContext context) {

        DataTaskMeta taskMeta = new DataTaskMeta();
        taskMeta.setDbName(context.getTable().getSrcDb());
        taskMeta.setTableName(context.getTable().getSrcTable());

        String imeiColumnName =
                context.getTable().getColumns().stream()
                    .filter(column -> column.getType()== ColumnType.BITMAP_COLUMN)
                    .map(column -> column.getSrcName())
                    .collect(Collectors.toList())
                    .get(0);
        taskMeta.setImeiColumnName(imeiColumnName);

        List<String> dimColumnNames =
            context.getTable().getColumns().stream()
                    .filter(column -> column.getType()== ColumnType.DIM_COLUMN)
                    .sorted(Comparator.comparingInt(Column::getIndex))
                    .map(column -> column.getSrcName())
                    .collect(Collectors.toList());
        taskMeta.setDimColumnNames(dimColumnNames);

        Column sliceColumn =
            context.getTable().getColumns().stream()
                    .filter(column -> column.getType()== ColumnType.SLICE_COLUMN)
                    .collect(Collectors.toList())
                    .get(0);
        taskMeta.setSliceColumnName(sliceColumn.getSrcName());

        DateTimeFormatter df = DateTimeFormatter.ofPattern(sliceColumn.getSrcPartColDateFormat().getFormat());
        if(sliceColumn.getSrcDataType()== ResourceColType.STRING) {
            taskMeta.setSliceColumnnValueLeft(SINGLE_QUOTATION + df.format(context.getLowerBound()) + SINGLE_QUOTATION);
            taskMeta.setSliceColumnValueRight(SINGLE_QUOTATION + df.format(context.getUpperBound()) + SINGLE_QUOTATION);
        }else{
            taskMeta.setSliceColumnnValueLeft(df.format(context.getLowerBound()));
            taskMeta.setSliceColumnValueRight(df.format(context.getUpperBound()));
        }

        taskMeta.setDictBasePath(context.getInvertedDictLocation());
        taskMeta.setOutputPath(context.getOutputLocation());

        taskMeta.setMaxRowPartition(defaultTaskConfig.getShardItems());//设置默认文件切分限制
        context.getJobProps().stream()
                .filter(props -> Props.KEY_SHARD_ITEMS.equals(props.getKey()))
                .forEach(props -> taskMeta.setMaxRowPartition(Integer.parseInt(props.getValue())));

        return taskMeta;
    }

    @Override
    public TaskStatus kill(String appId) throws JobException {
        throw new JobException(JobErrorCode.JOB_KILL_ERROR,  "kill operation is not allowed");
    }

    @Override
    public TaskStatus status(String appId) throws JobException {

        //向hadoop集群根据appid发送请求获取job执行进度
        String message = null;
        TaskStatus taskStatus = null;
        try {
            message = sendGet(defaultTaskConfig.getTrackUrl(), appId);

            //解析集群返回的信息
            String appJsonKey = "app";
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(message);

            if(rootNode.has(appJsonKey)){
                JsonNode appNode = rootNode.get(appJsonKey);
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                taskStatus = objectMapper.readValue(appNode.toString(), TaskStatus.class);
            }else {
                throw new JobException(JobErrorCode.JOB_MONITOR_ERROR, "job response error, message : %s", message);
            }
        } catch (IOException e) {
            throw new JobException(JobErrorCode.JOB_MONITOR_ERROR, e, "get job status error");
        }
        return taskStatus;
    }

    private Map<String, String> getTaskConfig(List<Props> jobProps) {

        Map<String, String> taskConfigMap = new HashMap<>();

        //设置默认配置
        taskConfigMap.put(SparkConfigConstant.DRIVER_MEMORY, defaultTaskConfig.getDriverMemory());
        taskConfigMap.put(SparkConfigConstant.EXECUTOR_CORES, defaultTaskConfig.getExecutorCores());
        taskConfigMap.put(SparkConfigConstant.EXECUTOR_INSTANCES, defaultTaskConfig.getExecutorInstances());
        taskConfigMap.put(SparkConfigConstant.EXECUTOR_MEMORY, defaultTaskConfig.getExecutorMemory());
        taskConfigMap.put(SparkConfigConstant.EXECUTOR_MEMORY_OVERHEAD, defaultTaskConfig.getMemoryOverhead());
        taskConfigMap.put(SparkConfigConstant.YARN_QUEUE, defaultTaskConfig.getQueue());

        //设置用户自定义配置
        jobProps.stream()
                .filter(props -> SparkConfigConstant.USER_CONFIG_WHITELIST.contains(props.getKey()))
                .forEach(props -> taskConfigMap.put(props.getKey(), props.getValue()));

        //根据executor配置计算默认并行度
        int parallelism = Integer.parseInt(taskConfigMap.get(SparkConfigConstant.EXECUTOR_INSTANCES))
                * Integer.parseInt(taskConfigMap.get(SparkConfigConstant.EXECUTOR_CORES))
                * defaultTaskConfig.getParallelism();
        taskConfigMap.put(SparkConfigConstant.DEFAULT_PARALLELISM, String.valueOf(parallelism));
        taskConfigMap.put(SparkConfigConstant.SQL_SHUFFLE_PARTITION, String.valueOf(parallelism));

        return taskConfigMap;
    }

    private String checkSparkHome(){
        String sparkHome = System.getenv("SPARK_HOME");
        if(sparkHome == null){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "submit spark job error, not found SPARK_HOME");
        }
        return sparkHome;
    }

    private void checkFile(String path){
        if(!new File(path).exists()){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "submit spark job error, %s is not exist", path);
        }
    }

    private String checkTagbaseConfDir(){
        String tagbaseConfDir = System.getenv("TAGBASE_CONF_DIR");
        if(tagbaseConfDir == null){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "submit spark job error, not found TAGBASE_CONF_DIR");
        }
        return tagbaseConfDir;
    }

    private String submitSparkJob(String taskMeta, Map<String,String> taskConfigMap, String mainClass) throws JobException {

        log.debug("submit Spark Job, taskMeta: {}", taskMeta);
        log.debug("submit Spark Job, taskConfigMap: {}", taskConfigMap);

        String sparkHome = checkSparkHome();
        String tagbaseConfDir = checkTagbaseConfDir();
        String coreSitePath = tagbaseConfDir + File.separator + "core-site.xml";
        String hdfsSitePath = tagbaseConfDir + File.separator + "hdfs-site.xml";
        checkFile(coreSitePath);
        checkFile(hdfsSitePath);

        System.setProperty("user.name", defaultTaskConfig.getUser());
        System.setProperty("HADOOP_USER_NAME", defaultTaskConfig.getUser());

        SparkAppHandle handle = null;
        String appid = null;
        try {
            SparkLauncher launcher  = new SparkLauncher()
                    .setSparkHome(sparkHome)
                    //重定向spark客户端的错误日志
                    .redirectError(ProcessBuilder.Redirect.appendTo(new File(defaultTaskConfig.getSparkClientErrorLog())))
                    .setAppResource(defaultTaskConfig.getJarPath())
                    .setMainClass(mainClass)
                    .addAppArgs(taskMeta)//传递hive表等json参数
                    .addFile(coreSitePath)//添加配置文件
                    .addFile(hdfsSitePath)
                    .setMaster(defaultTaskConfig.getMaster())
                    .setDeployMode(defaultTaskConfig.getDeployMode())
                    .setVerbose(defaultTaskConfig.isLogVerbose());

            taskConfigMap.forEach((key,value) -> launcher.setConf(key, value));//设置一系列job启动参数

            handle = launcher.startApplication();

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
        //提交失败
        if(appid == null){
            throw new JobException(JobErrorCode.JOB_SUBMIT_ERROR, "SparkLauncher error, please check %s", defaultTaskConfig.getSparkClientErrorLog());
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
