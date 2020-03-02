package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

import static com.oppo.tagbase.jobv2.JobErrorCode.DICT_NOT_CONTINUOUS;
import static com.oppo.tagbase.jobv2.JobErrorCode.TIME_BOUND_OVERFLOW;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobExecutable implements Executable {

    private Logger log = LoggerFactory.getLogger(getClass());

    private Job job;
    private JobFSM jobFSM;
    private List<Executable> taskChain;
    private MetadataJob metadataJob;

    public JobExecutable(Job job, MetadataJob metadataJob, List<Executable> taskChain) {
        this.job = job;
        this.taskChain = taskChain;
        this.metadataJob = metadataJob;
        this.jobFSM = JobFSM.of(job, metadataJob);
    }

    @Override
    public void perform() throws JobException {

        String previousThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(job.getId());

        try {

            log.info("Job {} starting", job.getName());

            checkDictJobPreConditions();
            checkDataJobPreConditions();

            jobFSM.toRunning();
            metadataJob.updateJobStartTime(job.getId(), LocalDateTime.now());

            for (Executable task : taskChain) {
                task.perform();
            }

            jobFSM.toSuccess();
            log.info("Job {} success", job.getName());

            metadataJob.updateJobEndTime(job.getId(), LocalDateTime.now());
        } catch (Exception e) {
            log.error("Job failed.", e);
            jobFSM.toFailed();

        } finally {
            Thread.currentThread().setName(previousThreadName);
        }
    }


    /**
     * keep dictionary is continuous.
     */
    // TODO Identifying the dict hole by jobs the fashion is a little trick.
    private void checkDictJobPreConditions() {
        if(JobType.DICTIONARY == job.getType()) {
            Job latestSuccessJob = metadataJob.getLatestDictJob(JobState.SUCCESS);
            if(latestSuccessJob != null) {
                if (!latestSuccessJob.getDataUpperTime().equals(job.getDataLowerTime())){
                    // fatal error Dictionary has hole
                    throw new JobException(DICT_NOT_CONTINUOUS, "Dictionary has hole, pls first fix the holes.");
                }
            }
        }
    }

    // TODO Identifying the dict hole by jobs the fashion is a little trick.
    private void checkDataJobPreConditions() {
        if(JobType.DATA == job.getType()) {
            // check whether the dictionary meet the job time bound.
            // listSuccessDictJobs must get more
            List<Job> jobList = metadataJob.listSuccessDictJobs(job.getDataLowerTime(), job.getDataUpperTime());
            Timeline timeline = JobUtil.makeJobTimeline(jobList);
            if(timeline.encloses(job.toRange())) {
                throw new JobException(TIME_BOUND_OVERFLOW, "The global dictionary can not meet the time bound, skip the building.");
            }
        }
    }

}
