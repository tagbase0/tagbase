package com.oppo.tagbase.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Created by daikai on 2020/2/16.
 */
public class DictJob implements AbstractJob {

    Logger log = LoggerFactory.getLogger(DictJob.class);

    @Override
    public boolean succeed(String jobId) {
        if (JobState.SUCCESS == new MetadataJob().getJob(jobId).getState()) {
            return true;
        }
        return false;
    }

    @Override
    public void buildDict(String dbName, String tableName) {
        Job job = iniJob(dbName, tableName);
        build(job);

        // 更新元数据模块内容
        new MetadataJob().completeJOb(job.getId(), JobState.SUCCESS, new Date(System.currentTimeMillis()));
    }

    @Override
    public String buildData(String dbName, String tableName, String lowerDate, String upperDate) {
        throw new UnsupportedOperationException("Dict build job  doesn't support build data !");
    }

    @Override
    public Job jobInfo(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName) {

        Job dictJob = new Job();

        String dictJobId = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());

        dictJob.setId(dictJobId);
        dictJob.setName(dbName + "_" + tableName + "_" + today);
        dictJob.setDbName(dbName);
        dictJob.setTableName(tableName);
        dictJob.setStartTime(new Date(System.currentTimeMillis()));
        dictJob.setLatestTask("");
        dictJob.setState(JobState.PENDING);
        dictJob.setType(JobType.DICTIONARY);

        new MetadataJob().addJob(dictJob);

        // 定义子任务 tasks
        iniTasks(dictJob);

        return dictJob;

    }

    private void iniTasks(Job dictJob) {
        // task invertedTask 初始化反向字典
        Task invertedTask = new Task();
        //TODO 2020/2/20  任务输出路径待指定
        String outputInverted = "";
        iniTask(dictJob.getId(), invertedTask, "InvertedDictBuildTask", (byte) 0, outputInverted);

        // task forwardTask 初始化正向字典
        Task forwardTask = new Task();
        String outputForward = "";
        iniTask(dictJob.getId(), forwardTask, "ForwardDictBuildTask", (byte) 1, outputForward);

        new MetadataJob().addTask(invertedTask);
        new MetadataJob().addTask(forwardTask);
    }

    private void iniTask(String jobId, Task task, String name, byte step, String output) {
        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());
        task.setId(new IdGenerator().nextQueryId(name, "yyyyMMdd"));
        task.setName(name + "_" + today);
        task.setJobId(jobId);
        task.setStep(step);
        task.setOutput(output);
        task.setState(TaskState.PENDING);
    }

    public void build(Job dictJob) {

        String jobId = dictJob.getId();
        log.info("Dictionary job {} is pending", jobId);

        // 若已准备好构建
        if (readytoBuild()) {

            buildDict(dictJob);
            log.info("Dictionary job {} is finished", jobId);
        } else {

            log.info("Skip to build Dictionary job {}  !", jobId);
        }

    }

    private boolean readytoBuild() {
        String jobIdtoday = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        if (JobState.RUNNING != new MetadataJob().getJob(jobIdtoday).getState() &&
                JobState.PENDING != new MetadataJob().getJob(jobIdtoday).getState()) {
            return true;
        }
        return false;
    }

    public void buildDict(Job dictJob) {
        dictJob.setState(JobState.RUNNING);
        log.info("Dictionary job {} is running", dictJob.getId());

        List<Task> tasks = dictJob.getTasks();
        int stepNum = tasks.size();

        JobState jobState = dictJob.getState();

        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; jobState != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // InvertedDictTask;
                        // 参数：写入分区地址location, dbName, tableName
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        //TODO 2020/2/16  调用反向字典Spark任务
                        TaskState state = getSparkState(task.getId());

                        new MetadataJob().completeTask(task.getId(),
                                task.getState(),
                                new Date(System.currentTimeMillis()),
                                task.getOutput());

                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // ForwardDictTask;
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        String fileLocationInverted = tasks.get(0).getOutput();

                        Dict dictForwardOld = new MetadataDict().getDict();
                        // 参数 fileLocationInverted, locationForward,
                        Dict dictForwardToday = buildDictForward(fileLocationInverted, dictForwardOld, task.getOutput());

                        // 元数据更新
                        String fileLocationForwardNew = dictForwardToday.getLocation();
                        task.setOutput(fileLocationForwardNew);
                        new MetadataJob().completeTask(task.getId(),
                                task.getState(),
                                new Date(System.currentTimeMillis()),
                                task.getOutput());

                        new MetadataDict().addDict(dictForwardToday);

                    }
                    break;
                default:
                    break;

            }
            i = i % stepNum;
        }

        new MetadataJob().completeJOb(dictJob.getId(), JobState.SUCCESS, new Date(System.currentTimeMillis()));

    }

    private TaskState getSparkState(String id) {

        return TaskState.SUCCESS;
    }

    private Dict buildDictForward(String locationInverted, Dict dictForwardOld, String locationForward) {

        Dict dict = new Dict();

        long numOld = dictForwardOld.getElementCount();
        // 1.先读取反向字典相关数据, 将今日新增数据生成<id, imei>的 kv 结构
        TreeMap<Long, String> mapInverted = readInvertedHdfs(locationInverted, numOld);

        // 2.将新增数据追加写入正向字典数据hdfs文件
        String locationForwardOld = dictForwardOld.getLocation();
        write2hdfs(mapInverted, locationForwardOld, locationForward);

        // 3.更新字典元数据
        dict.setLocation(locationForward);
        dict.setCreateDate(new Date(System.currentTimeMillis()));
        dict.setStatus(DictStatus.READY);
        dict.setElementCount(numOld + mapInverted.size());
        dict.setId(System.currentTimeMillis());
        dict.setType(DictType.FORWARD);
        dict.setVersion("");

        return dict;

    }

    public TreeMap readInvertedHdfs(String locationInverted, long numOld) {
        TreeMap<Long, String> mapInverted = new TreeMap<>();
        try (FileSystem fs = FileSystem.get(URI.create(locationInverted), new Configuration());
             FSDataInputStream fsr = fs.open(new Path(locationInverted));
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr))) {
            String line;
            String imei;
            long id;
            while ((line = bufferedReader.readLine()) != null) {
                // 此处确定imei和id的分隔符
                if (line.contains(" ")) {
                    imei = line.split(" ")[0];
                    id = Long.parseLong(line.split(" ")[1]);
                    if (numOld <= id) {
                        mapInverted.put(id, imei);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Read inverted dictionary from hdfs ERROR !");
        }
        return mapInverted;
    }

    public void write2hdfs(Map<Long, String> mapInverted, String location, String locationForward) {

        Path path = new Path(locationForward);
        try (FileSystem fs = path.getFileSystem(new Configuration());
             FSDataOutputStream output = fs.append(path)) {
            ObjectMapper objectMapper = new ObjectMapper();

            //如果此文件不存在则创建新文件
            if (!fs.exists(path)) {
                fs.createNewFile(path);
            } else {
                fs.delete(path, true);
            }

            for (Map.Entry<Long, String> entry : mapInverted.entrySet()) {
                output.write(objectMapper.writeValueAsString(entry.getValue()).getBytes("UTF-8"));
                //换行
                output.write("\n".getBytes("UTF-8"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
