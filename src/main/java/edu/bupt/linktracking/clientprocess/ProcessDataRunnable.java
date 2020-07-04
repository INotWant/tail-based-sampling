package edu.bupt.linktracking.clientprocess;

import edu.bupt.linktracking.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ProcessDataRunnable implements Runnable {

    private final BlockingQueue<List<String>> spanQueue;
    private final List<Map<String, List<String>>> ringCaches;
    private final BlockingQueue<Pair<Set<String>, Integer>> uploadQueue;

    private final Lock lockForRingCaches;
    private final Condition conditionForRingCaches;

    public ProcessDataRunnable(BlockingQueue<List<String>> spanQueue, List<Map<String, List<String>>> ringCaches, BlockingQueue<Pair<Set<String>, Integer>> uploadQueue, Lock lockForRingCaches, Condition conditionForRingCaches) {
        this.spanQueue = spanQueue;
        this.ringCaches = ringCaches;
        this.uploadQueue = uploadQueue;
        this.lockForRingCaches = lockForRingCaches;
        this.conditionForRingCaches = conditionForRingCaches;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        int batchPos = -1;
        int cachePos = 0;
        int ringCacheSize = ringCaches.size();
        Set<String> lastWrongTraceIds = null;
        Set<String> currWrongTraceIds = new HashSet<>();
        Map<String, List<String>> currCache = ringCaches.get(cachePos);

        try {
            while (true) {
                List<String> spans = spanQueue.take();

                if (spans.size() == 0) {
                    break;
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
                            currWrongTraceIds.add(traceId);
                        }
                    }
                }

                LOGGER.info("suc to process data block, batchPos: " + batchPos);

                if (batchPos >= 0) {
                    uploadQueue.put(new Pair<>(lastWrongTraceIds, batchPos));
                }

                lastWrongTraceIds = currWrongTraceIds;
                currWrongTraceIds = new HashSet<>();

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
            // because upload slow one
            if (lastWrongTraceIds != null && lastWrongTraceIds.size() > 0) {
                uploadQueue.put(new Pair<>(lastWrongTraceIds, batchPos));
            }

            LOGGER.info("exit process data thread");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
