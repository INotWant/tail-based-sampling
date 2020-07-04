package edu.bupt.linktracking.clientprocess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.SynchronousQueue;

public class ReadDataRunnable implements Runnable {
    private final String path;

    private final long rangeSize;
    private final long rangeStep;
    private long rangeValueStart;

    private long dataSize;

    private SynchronousQueue<List<String>> synQueue = new SynchronousQueue<>();

    public ReadDataRunnable(String path, long rangeSize, long rangeStep, long rangeValueStart) {
        this.path = path;
        this.rangeSize = rangeSize;
        this.rangeStep = rangeStep;
        this.rangeValueStart = rangeValueStart;
    }

    private String getRangeValue() {
        return "bytes=" + rangeValueStart + "-" + (rangeValueStart + rangeSize - 1);
    }

    private void addRangeValueStart() {
        this.rangeValueStart += rangeStep;
    }

    public SynchronousQueue<List<String>> getSynQueue() {
        return synQueue;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());
        HttpURLConnection httpConnection = null;

        int cacheSize = (int) rangeSize;
        byte[] cache = new byte[cacheSize];

        List<String> resultList = new LinkedList<>();

        try {
            URL url = new URL(path);

            int pos, readNum;
            String lastRemain = "";

            while (true) {
                LOGGER.info("start to read");

                httpConnection = (HttpURLConnection) url.openConnection();
                httpConnection.setDoOutput(false);
                httpConnection.setRequestProperty("Range", this.getRangeValue());
                httpConnection.connect();

                if (dataSize == 0) {
                    Map<String, List<String>> headers = httpConnection.getHeaderFields();
                    List<String> values = headers.get("Content-Range");
                    String value = values.get(0);
                    dataSize = Long.parseLong(value.substring(value.indexOf('/') + 1));
                }

                InputStream inputStream = httpConnection.getInputStream();

                pos = 0;
                while ((readNum = inputStream.read(cache, pos, cacheSize - pos)) != -1 && pos < cacheSize) {
                    pos += readNum;
                }

                String content = "";
                if (pos != 0) {
                    content += new String(cache, 0, pos);
                }

                String[] splits = content.split("\n");
                splits[0] = lastRemain + splits[0];

                int splitsLen = splits.length;
                int splitsAvailableLen;

                if (content.charAt(content.length() - 1) != '\n') {
                    lastRemain = splits[splitsLen - 1];
                    splitsAvailableLen = splitsLen - 1;
                } else {
                    lastRemain = "";
                    splitsAvailableLen = splitsLen;
                }

                LOGGER.info("suc to read a cache, size: " + pos);

                if (resultList.size() + splitsAvailableLen < 20000) {
                    for (int i = 0; i < splitsAvailableLen; i++) {
                        resultList.add(splits[i]);
                    }
                } else {
                    int j = 0;
                    for (int i = resultList.size(); i < 20000; i++) {
                        resultList.add(splits[j++]);
                    }
                    this.synQueue.put(resultList);
                    LOGGER.info("suc to enqueue DATA_STR_QUEUE!");
                    resultList = new LinkedList<>();

                    while (j < splitsAvailableLen) {
                        resultList.add(splits[j++]);
                    }
                }

                this.addRangeValueStart();
                if (this.rangeValueStart >= this.dataSize) {
                    if (resultList.size() > 0) {
                        this.synQueue.put(resultList);
                    }

                    this.synQueue.put(new LinkedList<>());
                    LOGGER.info("exit read data thread, data request completed");
                    return;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (httpConnection != null) {
                httpConnection.disconnect();
            }
        }
    }
}
