package com.miya.mqtt.client.sdk.concurrent;

import java.util.concurrent.*;

public class ExeService4Publish {
    private static ExecutorService executorService = null;

    public static ExecutorService getExecutorService() {
        if (executorService == null) {
            synchronized (ExeService4Publish.class) {
                if (executorService == null) {
                    executorService = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
                            10L, TimeUnit.SECONDS,
                            new SynchronousQueue<Runnable>());
                }
            }
        }
        return executorService;
    }
}
