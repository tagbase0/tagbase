package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import javax.inject.Inject;
import java.util.Date;

/**
 * Metadata service for job
 *
 * Created by wujianchao on 2020/2/17.
 */
public class MetadataJob {

    @Inject
    private MetadataConnector metadataConnector;

    public void createJob(Job job) {
        metadataConnector.createJob(job);
    }

    public void deleteJOb(String jobId) {
        metadataConnector.deleteJOb(jobId);
    }

    public void completeJOb(String jobId, JobState state, Date endTime) {
        metadataConnector.completeJOb(jobId, state, endTime);
    }

    public Job getJob(String jobId) {
        return metadataConnector.getJob(jobId);
    }

    public void createTask(Task task) {
        metadataConnector.createTask(task);
    }

    public void completeTask(String taskId, TaskState state, Date endTime) {
        metadataConnector.completeTask(taskId, state, endTime);
    }
}
