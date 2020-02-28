package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Metadata service for job
 *
 * Created by wujianchao on 2020/2/17.
 */
public class MetadataJob {

    @Inject
    private MetadataConnector metadataConnector;

    public void addJob(Job job) {
        metadataConnector.addJob(job);
    }

    public void deleteJOb(String jobId) {
        metadataConnector.deleteJOb(jobId);
    }

    public List<Job> listNotCompletedJob(String dbName, String tableName, LocalDateTime startTime, LocalDateTime endTime) {
        // TODO
        // not completed job : jobs in JobState.PENDING RUNNING FAILED SUSPEND state.
        return null;
    }

    public void completeJOb(String jobId, JobState state, LocalDateTime endTime) {
        metadataConnector.completeJOb(jobId, state, endTime);
    }

    public Job getJob(String jobId) {
        return metadataConnector.getJob(jobId);
    }

    public void addTask(Task task) {
        metadataConnector.addTask(task);
    }

    public void completeTask(String taskId, TaskState state, LocalDateTime endTime, String output) {
        metadataConnector.completeTask(taskId, state, endTime, output);
    }

    public void updateJob(Job job){
        metadataConnector.updateJob(job);
    }

    public void updateTask(Task task){
        metadataConnector.updateTask(task);
    }

    public Task getTask(String taskId) {
        return metadataConnector.getTask(taskId);
    }
}
