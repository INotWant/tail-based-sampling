package edu.bupt.linktracking.clientprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.SynchronousQueue;

public class ProcessDataRunnable implements Runnable {

    private final List<Map<String, List<String>>> ringCaches;
    private final SynchronousQueue<List<String>> spanSynQueue = new SynchronousQueue<>();
    private final SynchronousQueue<Set<String>> uploadSynQueue = new SynchronousQueue<>();

    public ProcessDataRunnable(List<Map<String, List<String>>> ringCaches) {
        this.ringCaches = ringCaches;
    }

    public SynchronousQueue<List<String>> getSpanSynQueue() {
        return spanSynQueue;
    }

    public SynchronousQueue<Set<String>> getUploadSynQueue() {
        return uploadSynQueue;
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
                List<String> spans = spanSynQueue.take();

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
            }

            LOGGER.info("exit process data thread");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
