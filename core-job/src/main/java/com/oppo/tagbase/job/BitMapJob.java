package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMapJob extends AbstractJob {

    private static final int RUNNING_JOBS_LIMIT = 50;

    @Override
    public boolean succeed(String jobId) {
        return false;
    }

    @Override
    public String build(String dbName, String tableName) {
        Table table = new Metadata().getTable(dbName, tableName);

        BitMapJob bitMapJob = new BitMapJob();
        bitMapJob.setStartTime(System.currentTimeMillis());

        String bitMapJobId = new IdGenerator().nextQueryId("BitMapBuildJob");
        bitMapJob.setId(bitMapJobId);

        String jobName = "BitMapBuildJob_" + table.getSrcDb() + "_" + table.getSrcTable() +
                table.getName() + "_" + table.getDesc();
        bitMapJob.setJobName(jobName);
        bitMapJob.setTableId(table.getId());
        bitMapJob.setState(State.PENDING);
        bitMapJob.setType(JobType.BITMAP);

        addPendingJob(bitMapJob);

        // 若反向字典以构建完成，且当前负载不高时，从pending队列取一个到running队列
        //
        while(succeed(DictJob.DICT_JOB_ID) &&
                AbstractJob.RUNNING_JOBS_QUEUE.size() <= RUNNING_JOBS_LIMIT){

            BitMapJob bitMapJobHead = (BitMapJob) AbstractJob.PENDING_JOBS_QUEUE.peek();

            addBitMapJob(bitMapJobHead);
            addBitMapTasks(bitMapJobHead);

            Future<Slice> slice = JOB_EXECUTORS.submit(new BitMaptTask(bitMapJobHead.getId()));

        }

        return bitMapJobId;

    }

    //TODO 2020/2/16 更新元数据模块相关信息

    public void addBitMapJob(BitMapJob bitMapJobHead) {
    }

    public void addBitMapTasks(BitMapJob bitMapJobHead) {

    }

    // 按 step 从小到大顺序返回所有子任务，step从0开始
    public List<BitMaptTask> getAllBuildTasks(String bitMapJobId){
        List<BitMaptTask> listBuildTasks = new ArrayList<>();

        return listBuildTasks;
    }
}
