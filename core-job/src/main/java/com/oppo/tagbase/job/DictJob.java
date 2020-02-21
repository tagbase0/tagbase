package com.oppo.tagbase.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
                        // 参数：写入HDFS地址, dbName, tableName
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);

                        long numOld = new MetadataDict().getDictElementCount();
                        String dbName = dictJob.getDbName();
                        String tableName = dictJob.getTableName();
                        String dayno = new Date(System.currentTimeMillis()).toString();
                        String locationInvertedDictOld = "";
                        String locationInvertedDictNew = "";

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

    private Dict buildDictForward(String locationInverted, Dict dictForwardOld, String locationHdfsForward) {

        Dict dict = new Dict();

        long numOld = dictForwardOld.getElementCount();
        // 1.先读取反向字典相关数据, 将今日新增数据生成<id, imei>的 kv 结构
        TreeMap<Long, String> mapInverted = readInvertedHdfs(locationInverted, numOld);

        // 2.将新增数据追加写入正向字典数据本地磁盘文件和hdfs文件
        String locationForwardDisk = "";
        write2Disk(mapInverted, locationForwardDisk);
        try {
            if (firstBuild()) {
                ForwardDictionaryWriter.createWriter(new File(locationForwardDisk));
            } else {
                ForwardDictionaryWriter.createWriterForExistedDict(new File(locationForwardDisk));
            }

        } catch (IOException e) {
            log.error("Error to ForwardDictionaryWriter !");
        }

        String locationHdfsForwardOld = dictForwardOld.getLocation();
        write2Hdfs(mapInverted, locationHdfsForwardOld, locationHdfsForward);

        // 3.更新字典元数据
        dict.setLocation(locationHdfsForward);
        dict.setCreateDate(new Date(System.currentTimeMillis()));
        dict.setStatus(DictStatus.READY);
        dict.setElementCount(numOld + mapInverted.size());
        dict.setId(System.currentTimeMillis());
        dict.setType(DictType.FORWARD);
        dict.setVersion("");

        return dict;

    }

    private boolean firstBuild() {
        if (new MetadataDict().getDictElementCount() == 0) {
            return true;
        }
        return false;
    }

    private void write2Disk(TreeMap<Long, String> mapInverted, String locationForwardDisk) {
        try {
            for (Map.Entry<Long, String> entry : mapInverted.entrySet()) {
                // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
                String str = entry.getValue();
                FileWriter writer = new FileWriter(locationForwardDisk, true);
                writer.write(System.getProperty("line.separator") + str);
                writer.close();
            }

        } catch (IOException e) {
            log.error("Error to write forward dictionary disk file ! ");
        }
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

    public void write2Hdfs(Map<Long, String> mapInverted, String locationHDFSForwardOld, String locationHDFSForwardNew) {

        Path pathSrc = new Path(locationHDFSForwardOld);
        Path pathDes = new Path(locationHDFSForwardNew);
        Configuration conf = new Configuration();
        try (FileSystem fs = pathDes.getFileSystem(conf);
             FSDataOutputStream output = fs.append(pathDes)) {

            // 1.复制旧的内容
            FileUtil.copy(fs, pathSrc,
                    fs, pathDes,
                    false, true, conf);

            ObjectMapper objectMapper = new ObjectMapper();

            //如果此文件不存在则创建新文件
            if (!fs.exists(pathDes)) {
                fs.createNewFile(pathDes);
            }

            // 2.追加新的内容
            for (Map.Entry<Long, String> entry : mapInverted.entrySet()) {
                output.write(objectMapper.writeValueAsString(entry.getValue()).getBytes("UTF-8"));
                //换行
                output.write(System.getProperty("line.separator").getBytes("UTF-8"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
