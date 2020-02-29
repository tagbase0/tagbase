package com.oppo.tagbase.jobv2;

import com.google.inject.Inject;
import com.oppo.tagbase.jobv2.spi.HiveMeta;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/2/29.
 */
public class DataJobExecutableMaker {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private TaskEngine engine;
    @Inject
    private StorageConnector storage;
    @Inject
    private MetadataJob metadataJob;
    @Inject
    private Metadata metadata;
    @Inject
    private MetadataDict metadataDict;

    public JobExecutable make(Job job) {

        List<Task> taskList = job.getTasks();

        List<Executable>  executableList = taskList.stream()
                .filter(task -> task.getState().isCompleted())
                .map(task -> makeTaskExecutable(job, task))
                .collect(Collectors.toList());

        return new JobExecutable(job, metadataJob, executableList);
    }

    private Executable makeTaskExecutable(Job job, Task task) {

        switch (task.getStep()) {
            case 0:
                return makeBuildingBitmapStep(task);
            case 1:
                return makeLoadDataToStorageStep(job, task);
            default:
                throw new JobException("Illegal dict task step: " + task.getStep());
        }
    }


    private Executable makeBuildingBitmapStep(Task task) {
        return () -> {

            TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
            try {

                taskFSM.toRunning();

                // TODO init HiveMeta
                HiveMeta hiveMeta = null;
                String appId = engine.submitTask(hiveMeta, JobType.DATA);

                task.setAppId(appId);
                metadataJob.updateTaskAppId(task.getId(), task.getAppId());

                TaskStatus status = null;

                while (!(status = engine.getTaskStatus(appId, JobType.DATA)).isDone()) {
                    TimeUnit.SECONDS.sleep(60);
                    log.debug("{} still running", appId);
                    status = engine.getTaskStatus(appId, JobType.DATA);
                }


                if (!status.isSuccess()) {
                    throw new JobException("external task %s failed, reason: %s", appId, status.getErrorMessage());
                }

                task.setEndTime(LocalDateTime.now());
                // TODO output = HFile location
                log.info("Bitmap data location {}", task.getOutput());
                task.setOutput(null);


                metadataJob.updateTask(task);
                taskFSM.toSuccess();

            } catch (Exception e) {
                taskFSM.toFailed();
                throw new JobException(e, "Task %s failed", task.getName());
            }
        };
    }

    private Executable makeLoadDataToStorageStep(Job job, Task task) {
        return () -> {

            TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
            try {

                taskFSM.toRunning();

                // 1. add slice to storage

                //Bitmap data location
                String previousTask = JobUtil.previousTask(job, task).getId();
                String dataLocation = metadataJob.getTask(previousTask).getOutput();

                String sliceSink = storage.addSlice(dataLocation);

                // 2. add slice to metadata

                Slice slice = new Slice();
                long tableId = metadata.getTable(job.getDbName(), job.getTableName()).getId();
                slice.setTableId(tableId);
                slice.setStartTime(job.getDataLowerTime());
                slice.setEndTime(job.getDataUpperTime());
                slice.setShardNum(1);
                slice.setSink(sliceSink);
                //TODO set value
                slice.setSinkCount(0);
                slice.setSinkCount(0);

                metadata.addSlice(slice);

                taskFSM.toSuccess();

            } catch (Exception e) {
                taskFSM.toFailed();
                throw new JobException(e, "Task %s failed", task.getName());
            }
        };
    }
}
