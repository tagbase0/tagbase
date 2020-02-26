package com.oppo.tagbase.job_v2;

import com.google.common.collect.Lists;
import com.oppo.tagbase.common.Pair;

import java.util.List;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class TaskChain<T> {

    private List<Pair<?, Executor<T>>> chain = Lists.newArrayList();


    public void addTask(Pair<?, Executor<T>> task) {
        chain.add(task);
    }

    public void perform() {

    }
}
