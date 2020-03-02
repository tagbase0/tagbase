package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata service for job
 * <p>
 * Created by wujianchao on 2020/2/17.
 */
public class MetadataJob {

    @Inject
    private MetadataConnector metadataConnector;

    public void addJob(Job job) {
        metadataConnector.addJob(job);
    }

    public void deleteJob(String jobId) {
        metadataConnector.deleteJob(jobId);
    }

    public List<Job> listNotCompletedJob(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        // not completed job : jobs in JobState.PENDING RUNNING FAILED SUSPEND state.
        return metadataConnector.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);
    }

    public void completeJob(String jobId, JobState state, LocalDateTime endTime) {
        metadataConnector.completeJob(jobId, state, endTime);
    }

    public Job getJob(String jobId) {
        return metadataConnector.getJob(jobId);
    }

    public Job getRunningDictJob() {
        return metadataConnector.getRunningDictJob();
    }

    public void addTask(Task task) {
        metadataConnector.addTask(task);
    }

    public void completeTask(String taskId, TaskState state, LocalDateTime endTime, String output) {
        metadataConnector.completeTask(taskId, state, endTime, output);
    }

    public void updateJob(Job job) {
        metadataConnector.updateJob(job);
    }


    public void updateTask(Task task) {
        metadataConnector.updateTask(task);
    }

    public void updateJobStatus(String jobId, JobState state) {
        metadataConnector.updateJobStatus(jobId, state);
    }

    public void updateTaskStatus(String id, TaskState state) {
        metadataConnector.updateTaskStatus(id, state);
    }

    public void updateTaskAppId(String id, String appId) {
        metadataConnector.updateTaskAppId(id, appId);
    }

    public Task getTask(String taskId) {
        return metadataConnector.getTask(taskId);
    }

    public Task getTask(String jobId, byte step) {
        return metadataConnector.getTask(jobId, step);
    }

    public List<Job> listPendingJobs() {
        return metadataConnector.listPendingJobs();
    }

    public void updateJobStartTime(String id, LocalDateTime startTime) {
        metadataConnector.updateJobStartTime(id, startTime);
    }

    public void updateJobEndTime(String id, LocalDateTime endTime) {
        metadataConnector.updateJobEndTime(id, endTime);
    }

    public void updateTaskStartTime(String id, LocalDateTime startTime) {
        metadataConnector.updateTaskStartTime(id, startTime);
    }

    public void updateTaskEndTime(String id, LocalDateTime endTime) {
        metadataConnector.updateTaskEndTime(id, endTime);
    }

    public void updateTaskOutput(String id, String output) {
        metadataConnector.updateTaskOutput(id, output);
    }


    public List<Job> listSuccessDictJobs(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        return metadataConnector.listSuccessDictJobs(dataLowerTime, dataUpperTime);
    }

    public int getPendingJobCount() {
        return metadataConnector.getPendingJobCount();
    }

    public Job getLatestDictJob(JobState... stateList) {
        return metadataConnector.getLatestDictJob(stateList);
    }
}
