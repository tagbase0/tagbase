package com.oppo.tagbase.jobv2;

import com.google.inject.Inject;
import com.oppo.tagbase.jobv2.spi.DataTaskContext;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    @Inject
    private JobConfig jobConfig;

    public JobExecutable make(Job job) {

        List<Task> taskList = job.getTasks();

        List<Executable> executableList = taskList.stream()
                .filter(task -> {
                    if(task.getState() == TaskState.RUNNING) {
                        log.info("resume task {}", task.getName());
                    }
                    return task.getState() == TaskState.RUNNING || task.getState() == TaskState.PENDING;
                })
                .map(task -> makeTaskExecutable(job, task))
                .collect(Collectors.toList());

        return new JobExecutable(job, metadataJob, executableList);
    }

    private Executable makeTaskExecutable(Job job, Task task) {

        switch (task.getStep()) {
            case 0:
                return makeBuildingBitmapStep(job, task);
            case 1:
                return makeLoadDataToStorageStep(job, task);
            default:
                throw new JobException("Illegal dict task step: " + task.getStep());
        }
    }


    private Executable makeBuildingBitmapStep(Job job, Task task) {
        return new TaskExecutable(task, metadataJob, () -> {

            try {
                //TODO add dict dir
                // init context
                DataTaskContext context = new DataTaskContext(task.getJobId(),
                        task.getId(),
                        metadata.getTable(job.getDbName(), job.getTableName()),
                        jobConfig,
                        job.getDataLowerTime(),
                        job.getDataUpperTime()
                );
                String appId = engine.buildData(context);

                task.setAppId(appId);
                metadataJob.updateTaskAppId(task.getId(), task.getAppId());

                TaskStatus status = null;

                while (!(status = engine.status(appId)).isDone()) {
                    TimeUnit.SECONDS.sleep(60);
                    // check task state
                    if (metadataJob.getTask(task.getId()).getState() != TaskState.RUNNING) {
                        log.warn("task {} was canceled by user", task.getId());
                        engine.kill(appId);
                        return null;
                    }
                    log.debug("{} still running", appId);
                    status = engine.status(appId);
                }


                if (!status.isSuccess()) {
                    throw new JobException("external task %s failed, reason: %s", appId, status.getDiagnostics());
                }

                metadataJob.updateTaskOutput(task.getId(), context.getOutputLocation());

                log.info("Bitmap data location {}", context.getOutputLocation());

                metadataJob.updateJobProgress(job.getId(), 0.5f);
                return null;

            } catch (IOException | InterruptedException e) {
                throw new JobException("error when get external task %s status", task.getAppId());
            }
        });

    }

    private Executable makeLoadDataToStorageStep(Job job, Task task) {
        return new TaskExecutable(task, metadataJob, () -> {

            // 1. add slice to storage

            //Bitmap data location
            String previousTask = JobUtil.previousTask(job, task).getId();
            String dataLocation = metadataJob.getTask(previousTask).getOutput();

            String sliceSink = storage.addSlice(dataLocation);

            // 2. add slice to metadata

            Slice slice = new Slice();
            Table table = metadata.getTable(job.getDbName(), job.getTableName());
            slice.setTableId(table.getId());
            if (TableType.TAG == table.getType()) {
                slice.setStartTime(LocalDateTime.MIN);
                slice.setStartTime(LocalDateTime.MAX);
            } else if (TableType.ACTION == table.getType()) {
                slice.setStartTime(job.getDataLowerTime());
                slice.setEndTime(job.getDataUpperTime());
            }
            //TODO add to config
            slice.setShardNum(1);
            slice.setSink(sliceSink);
            //TODO set value
            slice.setSinkCount(0);
            //TODO set value
            slice.setSinkSizeMb(0);

            metadata.addSlice(slice);
            metadataJob.updateJobProgress(job.getId(), 1.0f);
            return null;
        });
    }
}
