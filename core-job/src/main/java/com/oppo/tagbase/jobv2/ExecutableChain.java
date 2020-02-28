package com.oppo.tagbase.jobv2;

import com.google.common.collect.Lists;
import com.oppo.tagbase.common.Pair;

import java.util.List;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class ExecutableChain<T> {

    private List<Pair<Executable, Executor<T>>> chain = Lists.newArrayList();


    public void addTask(Pair<Executable, Executor<T>> task) {
        chain.add(task);
    }

    public void perform() {

        for (Pair<Executable, Executor<T>> p : chain) {
//            p.getRight().perform(p.getLeft());
        }
    }

}
