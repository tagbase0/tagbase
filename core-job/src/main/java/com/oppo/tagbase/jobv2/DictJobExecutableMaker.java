package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.oppo.tagbase.common.util.BytesUtil;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.extension.spi.FileSystem;
import com.oppo.tagbase.extension.spi.Reader;
import com.oppo.tagbase.extension.spi.Writer;
import com.oppo.tagbase.jobv2.spi.DictTaskContext;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.jobv2.spi.TaskStatus;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.DictStatus;
import com.oppo.tagbase.meta.obj.DictType;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
    @Inject
    private DictHiveInputConfig dictHiveInputConfig;
    @Inject
    private JobConfig jobConfig;
    @Inject
    private FileSystem fileSystem;

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
        return new TaskExecutable(task, metadataJob, () -> {

            try {

                // init context
                DictTaskContext context = new DictTaskContext(job.getId(),
                        task.getId(),
                        dictHiveInputConfig,
                        jobConfig,
                        metadataDict.getDictElementCount(),
                        job.getDataLowerTime(),
                        job.getDataUpperTime()
                );

                // submit task to remote engine
                String appId = engine.buildDict(context);

                task.setAppId(appId);
                metadataJob.updateTaskAppId(task.getId(), task.getAppId());

                TaskStatus status = null;

                while (!(status = engine.status(appId)).isDone()) {
                    TimeUnit.SECONDS.sleep(60);
                    log.debug("{} still running", appId);
                    status = engine.status(appId);
                }

                if (!status.isSuccess()) {
                    throw new JobException("external task %s failed, reason: %s", appId, status.getErrorMessage());
                }

                // update task output
                metadataJob.updateTaskEndTime(task.getId(), LocalDateTime.now());
                metadataJob.updateTaskOutput(task.getId(), context.getOutputLocation());

                log.info("Dict output location {}", context.getOutputLocation());

                return null;

            } catch (IOException | InterruptedException e) {
                throw new JobException("error when get external task %s status", task.getAppId());
            }
        });
    }

    private Executable makeBuildingForwardDictStep(Job job, Task task) {
        return new TaskExecutable(task, metadataJob, () -> {

            ForwardDictionaryWriter writer = null;
            String forwardDictLocalPath = null;

            try {

                // 1. read increasing entries

                Task previousTask = metadataJob.getTask(task.getId());
                String invertedDictLocation = previousTask.getOutput();
                SortedMap<Long, String> incInvertedDictEntries = loadIncEntry(invertedDictLocation);

                // 2. create a dict writer

                Dict currentForwardDict = metadataDict.getDict();
                forwardDictLocalPath = makeForwardDictName();
                if (currentForwardDict == null) {
                    // first building
                    writer = ForwardDictionaryWriter.createWriter(new File(forwardDictLocalPath));
                } else {
                    //daily building
                    // load currentForwardDict from remote to local disk.
                    copyRemoteForwardDictToLocal(currentForwardDict.getLocation(), forwardDictLocalPath);
                    writer = ForwardDictionaryWriter.createWriterForExistedDict(new File(forwardDictLocalPath));
                }

                // 3. append dict entries

                for (Map.Entry<Long, String> e : incInvertedDictEntries.entrySet()) {
                    //check dict consistency
                    Preconditions.checkArgument(
                            writer.add(BytesUtil.toUTF8Bytes(e.getValue())) == e.getKey(),
                            "inconsistent index between Inverted and Forward dictionaries."
                    );
                }

                writer.complete();


                // 4. push to remote file system

                // TODO get path
                String newDictRemoteLocation = null;
                Writer remoteWriter = fileSystem.createWriter(newDictRemoteLocation);
//                remoteWriter.write();

                // 5. update dict metadata

                Dict dict = new Dict();
                dict.setLocation(newDictRemoteLocation);
                dict.setType(DictType.FORWARD);
                dict.setElementCount(incInvertedDictEntries.lastKey());
                dict.setStatus(DictStatus.READY);
                dict.setCreateDate(LocalDateTime.now());
                metadataDict.addDict(dict);

            } finally {
                // 6. clean up
                JobUtil.deleteLocalFile(forwardDictLocalPath);
            }

            return null;
        });
    }

    private void copyRemoteForwardDictToLocal(String remotePath, String localPath) {
        try (Reader remoteFileReader = fileSystem.createReader(remotePath);) {
//            remoteFileReader.rea
            //TODO
        } catch (IOException e) {
            throw new JobException(e, "Error when copy remote forward dict to local.");
        }
    }

    /**
     * inverted dict file schema "id,entry"
     */
    private SortedMap<Long, String> loadIncEntry(String invertedDictLocation) {

        try (Reader invertedDictReader = fileSystem.createReader(invertedDictLocation);
             BufferedReader bf = new BufferedReader(
                     new InputStreamReader(new ByteArrayInputStream(invertedDictReader.readFully())))) {

            ImmutableSortedMap.Builder<Long, String> ret = new ImmutableSortedMap.Builder<Long, String>(Long::compareTo);

            String line = null;
            while ((line = bf.readLine()) != null) {
                String[] parts = line.split(",");
                ret.put(Long.parseLong(parts[1]), parts[0]);
            }

            return ret.build();

        } catch (IOException e) {
            throw new JobException(e, "Error when load increasing inverted dict entries.");
        }

    }
}
