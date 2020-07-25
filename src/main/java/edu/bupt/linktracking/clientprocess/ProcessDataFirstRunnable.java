package edu.bupt.linktracking.clientprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

public class ProcessDataFirstRunnable implements Runnable {

    private final SynchronousQueue<String> dataBlockSynQueue;
    private final SynchronousQueue<List<String>> spansSynQueue;

    public ProcessDataFirstRunnable(SynchronousQueue<String> dataBlockSynQueue, SynchronousQueue<List<String>> spansSynQueue) {
        this.dataBlockSynQueue = dataBlockSynQueue;
        this.spansSynQueue = spansSynQueue;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        String lastRemain = "";
        List<String> spanList = new LinkedList<>();

        try {
            while (true) {
                LOGGER.info("wait a data block");

                String dataBlock = dataBlockSynQueue.take();
                if ("".equals(dataBlock)) {
                    if (spanList.size() > 0) {
                        LOGGER.info("suc to get the last batch spans");
                        this.spansSynQueue.put(spanList);
                        LOGGER.info("suc to put the last batch spans");
                    }
                    this.spansSynQueue.put(new LinkedList<>());
                    LOGGER.info("exit process data thread 1");
                    return;
                }

                String[] spans = dataBlock.split("\n");
                spans[0] = lastRemain + spans[0];

                int spansLen = spans.length;
                int spansAvailableLen;

                if (dataBlock.charAt(dataBlock.length() - 1) != '\n') {
                    lastRemain = spans[spansLen - 1];
                    spansAvailableLen = spansLen - 1;
                } else {
                    lastRemain = "";
                    spansAvailableLen = spansLen;
                }

                if (spanList.size() + spansAvailableLen < 20000) {
                    for (int i = 0; i < spansAvailableLen; i++) {
                        spanList.add(spans[i]);
                    }
                } else {
                    int j = 0;
                    for (int i = spanList.size(); i < 20000; i++) {
                        spanList.add(spans[j++]);
                    }
                    LOGGER.info("suc to get 20000 spans");
                    this.spansSynQueue.put(spanList);
                    LOGGER.info("suc to put 20000 spans");

                    spanList = new LinkedList<>();

                    while (j < spansAvailableLen) {
                        spanList.add(spans[j++]);
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
