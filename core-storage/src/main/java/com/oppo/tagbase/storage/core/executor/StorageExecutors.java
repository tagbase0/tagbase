package com.oppo.tagbase.storage.core.executor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageExecutors {

    public static ExecutorService newThreadPool(int coreThreads, int maxThreads, long keepAliveTime, int queueSize) {

        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(queueSize);
        return new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue, new StorageThreadFactory());

    }


    static class StorageThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        StorageThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "storage-pool-" + poolNumber.getAndIncrement() + "-thread-";
        }
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
