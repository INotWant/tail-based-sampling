package edu.bupt.linktracking.clientprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ProcessDataSecondRunnable implements Runnable {

    private final List<Map<String, List<String>>> ringCaches;
    private final SynchronousQueue<List<String>> spansSynQueue;
    private final SynchronousQueue<Set<String>> uploadSynQueue;

    private final Lock lockForRingCaches;
    private final Condition conditionForRingCaches;

    public final static Set<String> EXIT_FLAG_SET = new HashSet<>();

    public ProcessDataSecondRunnable(List<Map<String, List<String>>> ringCaches, SynchronousQueue<List<String>> spansSynQueue, SynchronousQueue<Set<String>> uploadSynQueue, Lock lockForRingCaches, Condition conditionForRingCaches) {
        this.ringCaches = ringCaches;
        this.spansSynQueue = spansSynQueue;
        this.uploadSynQueue = uploadSynQueue;
        this.lockForRingCaches = lockForRingCaches;
        this.conditionForRingCaches = conditionForRingCaches;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        int batchPos = 0;
        int cachePos = 0;
        int ringCacheSize = ringCaches.size();

        Set<String> wrongTraceIds = new HashSet<>();

        Map<String, List<String>> currCache = ringCaches.get(cachePos);

        try {
            while (true) {
                LOGGER.info("wait a spans");

                List<String> spans = spansSynQueue.take();

                if (spans.size() == 0) {
                    uploadSynQueue.put(EXIT_FLAG_SET);
                    LOGGER.info("exit process data thread 2");
                    return;
                }

                LOGGER.info("start to process data block, batchPos: " + batchPos);

                for (String span : spans) {
                    int indexF = span.indexOf('|');
                    int indexL = span.lastIndexOf('|');
                    if (indexF >= 0 && indexF != indexL) {
                        String traceId = span.substring(0, indexF);

                        List<String> spanList = currCache.computeIfAbsent(traceId, k -> new LinkedList<>());
                        spanList.add(span);
                        if (span.indexOf("error=1", indexL + 1) >= 0 ||
                                (span.indexOf("http.status_code=", indexL + 1) >= 0 && span.indexOf("http.status_code=200", indexL + 1) == -1)) {
                            wrongTraceIds.add(traceId);
                        }
                    }
                }

                LOGGER.info("suc to process data block, batchPos: " + batchPos);

                uploadSynQueue.put(wrongTraceIds);
                wrongTraceIds = new HashSet<>();

                ++batchPos;
                cachePos = ++cachePos == ringCacheSize ? 0 : cachePos;
                currCache = ringCaches.get(cachePos);

                if (currCache.size() > 0) {
                    long startTime = System.currentTimeMillis();
                    LOGGER.warn("cache conflict!");
                    lockForRingCaches.lock();
                    try {
                        while (currCache.size() > 0) {
                            conditionForRingCaches.await();
                        }
                    } finally {
                        lockForRingCaches.unlock();
                    }
                    long endTime = System.currentTimeMillis();
                    LOGGER.info("suc to clear cache, waiting time: " + (endTime - startTime));
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
