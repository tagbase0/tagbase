package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.oppo.tagbase.common.util.BytesUtil;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.jobv2.spi.HiveMeta;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.oppo.tagbase.jobv2.JobUtil.makeForwardDictName;

/**
 * Created by wujianchao on 2020/2/29.
 */
public class DictJobExecutableMaker {

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

        List<Executable> executableList = taskList.stream()
                .filter(task -> task.getState().isCompleted())
                .map(task -> makeTaskExecutable(job, task))
                .collect(Collectors.toList());

        return new JobExecutable(job, metadataJob, executableList);
    }

    private Executable makeTaskExecutable(Job job, Task task) {
        switch (task.getStep()) {
            case 0:
                return makeBuildingInvertedDictStep(job, task);
            case 1:
                return makeBuildingForwardDictStep(job, task);
            default:
                throw new JobException("Illegal dict task step: " + task.getStep());
        }

    }

    private Executable makeBuildingInvertedDictStep(Job job, Task task) {
        return () -> {

            TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
            try {

                taskFSM.toRunning();

                metadataJob.updateTaskStartTime(task.getId(), LocalDateTime.now());

                // TODO init HiveMeta
                HiveMeta hiveMeta = null;
                String appId = engine.submitTask(hiveMeta, JobType.DICTIONARY);

                task.setAppId(appId);
                metadataJob.updateTaskAppId(task.getId(), task.getAppId());

                TaskStatus status = null;

                while (!(status = engine.getTaskStatus(appId, JobType.DICTIONARY)).isDone()) {
                    TimeUnit.SECONDS.sleep(60);
                    log.debug("{} still running", appId);
                    status = engine.getTaskStatus(appId, JobType.DICTIONARY);
                }


                if (!status.isSuccess()) {
                    throw new JobException("external job %s failed, reason: %s", appId, status.getErrorMessage());
                }

                // TODO output = inverted dict location
                metadataJob.updateTaskOutput(task.getId(), null);

                taskFSM.toSuccess();

            } catch (Exception e) {
                taskFSM.toFailed();
                throw new JobException(e, "Task %s failed", task.getName());
            } finally {
                metadataJob.updateTaskEndTime(task.getId(), LocalDateTime.now());
            }
        };
    }

    private Executable makeBuildingForwardDictStep(Job job, Task task) {
        return () -> {

            TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
            try {

                taskFSM.toRunning();

                Dict currentForwardDict = metadataDict.getDict();

                ForwardDictionaryWriter writer = null;
                String currentForwardDictPath = null;


                //TODO
                Task previousTask = metadataJob.getTask(task.getId());

                String invertedDictLocation = previousTask.getOutput();
                TreeMap<Long, String> incEntries = null;
                // TODO read increasing entries
                // incEntries = loadIncEntry(invertedDictLocation)

                if (currentForwardDict == null) {
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

                for (Map.Entry<Long, String> e : incEntries.entrySet()) {
                    //check dict consistency
                    Preconditions.checkArgument(
                            writer.add(BytesUtil.toUTF8Bytes(e.getValue())) == e.getKey(),
                            "inconsistent index between Inverted and Forward dictionaries."
                    );
                }

                writer.complete();
                taskFSM.toSuccess();

            } catch (Exception e) {
                taskFSM.toFailed();
                throw new JobException(e, "Task %s failed", task.getName());
            }
        };
    }
}
