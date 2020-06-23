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
    @SuppressWarnings("unchecked")
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        int batchPos = -1;
        int cachePos = 0;
        int ringCacheSize = ringCaches.size();
        Set[] wrongTraceIdsArr = new Set[]{new HashSet<String>(32), new HashSet<String>(32)};
        int wrongTraceIdsIndex = 0, lastWrongTraceIdsIndex = 1;
        Map<String, List<String>> currCache = ringCaches.get(cachePos);
        String lastCacheBlockRemain = "";

        try {
            while (true) {
                List<String> spans = spanQueue.take();

                if (spans.size() == 0) {
                    break;
                }

                LOGGER.info("start to process data block, batchPos: " + batchPos);

                for (int i = 0; i < spans.size(); i++) {
                    String span = spans.get(i);
                    if (i == 0) {
                        span = lastCacheBlockRemain + span;
                    } else if (i == spans.size() - 1) { // there is a certain probability of error
                        lastCacheBlockRemain = span;
                        continue;
                    }
                    int indexF = span.indexOf('|');
                    int indexL = span.lastIndexOf('|');
                    if (indexF >= 0 && indexF != indexL) {
                        String traceId = span.substring(0, indexF);

                        List<String> spanList = currCache.computeIfAbsent(traceId, k -> new ArrayList<>());

                        spanList.add(span);
                        if (span.indexOf("error=1", indexL + 1) >= 0 ||
                                (span.indexOf("http.status_code=", indexL + 1) >= 0 && span.indexOf("http.status_code=200", indexL + 1) == -1)) {
                            if (!wrongTraceIdsArr[lastWrongTraceIdsIndex].contains(traceId)) {
                                wrongTraceIdsArr[wrongTraceIdsIndex].add(traceId);
                            } else {
                                LOGGER.info("requested last time!");
                            }
                        }
                    }
                }

                LOGGER.info("suc to process data block, batchPos: " + batchPos);

                lastWrongTraceIdsIndex = 1 - wrongTraceIdsIndex;
                Set<String> lastWrongTraceIdSet = wrongTraceIdsArr[lastWrongTraceIdsIndex];
                if (batchPos >= 0) {
                    uploadQueue.put(new Pair<>(new HashSet<>(lastWrongTraceIdSet), batchPos));
                    lastWrongTraceIdSet.clear();
                }

                wrongTraceIdsIndex = 1 - wrongTraceIdsIndex;
                lastWrongTraceIdsIndex = 1 - wrongTraceIdsIndex;
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
            Set<String> lastWrongTraceIdSet = wrongTraceIdsArr[lastWrongTraceIdsIndex];
            if (lastWrongTraceIdSet.size() > 0) {
                uploadQueue.put(new Pair<>(lastWrongTraceIdSet, batchPos));
            }

            LOGGER.info("exit process data thread");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
