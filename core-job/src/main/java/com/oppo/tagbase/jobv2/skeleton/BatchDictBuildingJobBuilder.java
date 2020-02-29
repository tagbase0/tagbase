package com.oppo.tagbase.jobv2.skeleton;

import com.google.common.base.Preconditions;
import com.oppo.tagbase.common.util.BytesUtil;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.jobv2.JobException;
import com.oppo.tagbase.jobv2.spi.HiveMeta;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class BatchDictBuildingJobBuilder {

    public static List<Executable> build(Job job,
            TaskEngine engine,
            MetadataJob metadataJob,
            MetadataDict metadataDict) {

        List<Task> taskList = job.getTasks();

        return taskList.stream()
                .filter(task -> task.getState() != TaskState.SUCCESS)
                .map(task -> makeTaskExecutable(job, task, engine, metadataJob, metadataDict))
                .collect(Collectors.toList());
    }

    private static Executable makeTaskExecutable(Job job,
                                                 Task task,
                                                 TaskEngine engine,
                                                 MetadataJob metadataJob,
                                                 MetadataDict metadataDict) {
        switch (task.getStep()) {
            case 0:
                return makeBuildingInvertedDictStep(job, task, engine, metadataJob);
            case 1:
                return makeBuildingForwardDictStep(job, task, metadataJob, metadataDict);
            default:
                throw new JobException("Illegal dict task step: " + task.getStep());
        }

    }

    private static Executable makeBuildingInvertedDictStep(Job job,
                                                           Task task,
                                                           TaskEngine engine,
                                                           MetadataJob metadataJob) {
        return () -> {
            try {

                metadataJob.updateTaskStatus(task.getId(), TaskState.RUNNING);

                // TODO make HiveMeta
                HiveMeta hiveMeta = null;
                String appId = engine.submitTask(hiveMeta, JobType.DICTIONARY);
                TaskStatus status = null;

                while (!(status = engine.getTaskStatus(appId, JobType.DICTIONARY)).isDone()) {
                    status = engine.getTaskStatus(appId, JobType.DICTIONARY);
                }

                Task clonedTask = task.clone();

                if(status.isSuccess()) {
                    clonedTask.setEndTime(LocalDateTime.now());
                    clonedTask.setAppId(appId);
                    // TODO output = inverted dict location
                    clonedTask.setOutput(null);
                    clonedTask.setState(TaskState.SUCCESS);
                } else {
                    clonedTask.setState(TaskState.FAILED);
                }

                metadataJob.updateTask(clonedTask);
                metadataJob.updateTaskStatus(task.getId(), clonedTask.getState());

            } catch (Exception e) {
                metadataJob.updateTaskStatus(task.getId(), TaskState.FAILED);
                throw new JobException(e, "");
            }
        };
    }
    private static Executable makeBuildingForwardDictStep(Job job,
                                                          Task task,
                                                          MetadataJob metadataJob,
                                                          MetadataDict metadataDict) {
        return () -> {

            try {

                metadataJob.updateTaskStatus(task.getId(), TaskState.RUNNING);
                Dict currentForwardDict = metadataDict.getDict();

                ForwardDictionaryWriter writer = null;
                String currentForwardDictPath = null;


                //TODO
                Task previousTask = metadataJob.getTask(task.getId());

                String invertedDictLocation = previousTask.getOutput();
                TreeMap<Long, String> incEntries = null;
                // TODO read increasing entries
                // incEntries = loadIncEntry(invertedDictLocation)

                long currentDictSize = metadataDict.getDictElementCount();
                checkDictConsistency(incEntries, currentDictSize);


                if(currentForwardDict ==  null) {
                    // first building
                    // TODO local path
                    currentForwardDictPath = makeForwardDictName();
                    writer = ForwardDictionaryWriter.createWriter(new File(currentForwardDictPath));
                } else {
                    //daily building

                    //TODO  load currentForwardDict from HDFS to local disk.
                    currentForwardDictPath = null;
                    writer = ForwardDictionaryWriter.createWriterForExistedDict(new File(currentForwardDictPath));
                }

                for(Map.Entry<Long, String> e : incEntries.entrySet()) {
                    Preconditions.checkArgument(
                            writer.add(BytesUtil.toUTF8Bytes(e.getValue())) == e.getKey(),
                            "inconsistent index between Inverted and Forward dictionaries."
                    );
                }

                metadataJob.updateTaskStatus(task.getId(), TaskState.SUCCESS);

            } catch (Exception e) {
                metadataJob.updateTaskStatus(task.getId(), TaskState.FAILED);
                throw new JobException(e, "");
            }
        };
    }

    private static void checkDictConsistency(TreeMap<Long, String> incEntries, long currentDictSize) {
        //TODO
//        Preconditions.check(incEntries.firstKey() == currentDictSize, "Dictionary size ");
    }


    public static String makeForwardDictName() {
        return System.getProperty("user.dir")
                + File.separator
                + "dict"
                + File.separator
                + "dict-forward-"
                + LocalDate.now()
                + ThreadLocalRandom.current().nextInt(100)
                ;
    }



}
