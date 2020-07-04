package edu.bupt.linktracking.clientprocess;

import edu.bupt.linktracking.Constants;
import edu.bupt.linktracking.Main;
import edu.bupt.linktracking.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static edu.bupt.linktracking.Main.getDataSourcePort;
import static edu.bupt.linktracking.Main.responseDataSourceReady;

public class MainRunnable implements Runnable {
    private static List<Map<String, List<String>>> RING_CACHES = new ArrayList<>();
    private static Lock LOCK_FOR_RING_CACHES = new ReentrantLock();
    private static Condition CONDITION_FOR_RING_CACHES = LOCK_FOR_RING_CACHES.newCondition();

    private static BlockingQueue<Pair<Set<String>, Integer>> UPLOAD_QUEUE = new LinkedBlockingQueue<>();

    // size of the ring cache
    private static int CACHE_TOTAL = 8;

    static {
        for (int i = 0; i < CACHE_TOTAL; i++) {
            RING_CACHES.add(new ConcurrentHashMap<>());
        }
    }

    private String getDataPath() {
        return 8000 == Main.LISTEN_PORT ? "http://localhost:" + Main.DATA_SOURCE_PORT + "/trace1.data" :
                "http://localhost:" + Main.DATA_SOURCE_PORT + "/trace2.data";
    }

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

            // start process data thread
            ProcessDataRunnable processDataRunnable1 = new ProcessDataRunnable(RING_CACHES);
            Thread processDataThread1 = new Thread(processDataRunnable1, "ProcessDataThread1");
            processDataThread1.start();
            ProcessDataRunnable processDataRunnable2 = new ProcessDataRunnable(RING_CACHES);
            Thread processDataThread2 = new Thread(processDataRunnable2, "ProcessDataThread2");
            processDataThread2.start();

            // start listen LISTEN_PORT (target: 1) communicate with data source; 2) communicate with backend for uploading span)
            serverSocket = new ServerSocket(Main.LISTEN_PORT);
            Socket dataSourceSocket = serverSocket.accept();
            responseDataSourceReady(dataSourceSocket);
            dataSourceSocket = serverSocket.accept();
            Main.DATA_SOURCE_PORT = getDataSourcePort(dataSourceSocket);
            if (Main.DATA_SOURCE_PORT == -1) {
                LOGGER.error("fail to get data source port");
            } else {
                LOGGER.error("suc to get data source port: " + Main.DATA_SOURCE_PORT);
            }

            // start upload thread
            int uploadRemotePort = Main.LISTEN_PORT == 8000 ? 8003 : 8004;
            new Thread(new UploadWrongTraceIdsRunnable(uploadRemotePort, UPLOAD_QUEUE), "UploadWrongTraceIdsThread").start();

            // start read data thread (need DATA_SOURCE_PORT)
            String dataPath = getDataPath();
            long rangeSize = 6 * Constants.MB_SIZE;
            long rangeStep = 6 * Constants.MB_SIZE;
            long rangeValueStart = 0;
            ReadDataRunnable readDataRunnable1 = new ReadDataRunnable(dataPath, rangeSize, rangeStep, rangeValueStart);
            new Thread(readDataRunnable1, "ReadDataThread1").start();

            // wait backend connection
            Socket backendSocket = serverSocket.accept();
            new Thread(new QuerySpanServiceRunnable(backendSocket, RING_CACHES, LOCK_FOR_RING_CACHES, CONDITION_FOR_RING_CACHES),
                    "QuerySpanServiceThread").start();

            // bridge read thread & process thread
            SynchronousQueue<List<String>> queue1 = readDataRunnable1.getSynQueue();

            SynchronousQueue<List<String>> spanSynQueue1 = processDataRunnable1.getSpanSynQueue();
            SynchronousQueue<List<String>> spanSynQueue2 = processDataRunnable2.getSpanSynQueue();
            SynchronousQueue<Set<String>> uploadSynQueue1 = processDataRunnable1.getUploadSynQueue();
            SynchronousQueue<Set<String>> uploadSynQueue2 = processDataRunnable2.getUploadSynQueue();

            Set<String> lastWrongTraceIds = null, currWrongTraceIds;
            int batchPos = -1;
            int cachePos = 0;
            int ringCacheSize = RING_CACHES.size();

            Map<String, List<String>> currCache = RING_CACHES.get(cachePos);

            while (true) {
                List<String> dataBlockStr1 = queue1.take();
                if (dataBlockStr1.size() > 0) {
                    Pair<List<String>, List<String>> pair = splitList(dataBlockStr1);

                    if (currCache.size() > 0) {
                        long startTime = System.currentTimeMillis();
                        LOGGER.warn("cache conflict!");
                        LOCK_FOR_RING_CACHES.lock();
                        try {
                            while (currCache.size() > 0) {
                                CONDITION_FOR_RING_CACHES.await();
                            }
                        } finally {
                            LOCK_FOR_RING_CACHES.unlock();
                        }
                        long endTime = System.currentTimeMillis();
                        LOGGER.info("suc to clear cache, waiting time: " + (endTime - startTime));
                    }

                    spanSynQueue1.put(pair.first);
                    spanSynQueue2.put(pair.second);

                    currWrongTraceIds = uploadSynQueue1.take();
                    currWrongTraceIds.addAll(uploadSynQueue2.take());

                    if (lastWrongTraceIds != null) {
                        UPLOAD_QUEUE.put(new Pair<>(lastWrongTraceIds, batchPos));
                    }

                    lastWrongTraceIds = currWrongTraceIds;
                    ++batchPos;
                    cachePos = ++cachePos == ringCacheSize ? 0 : cachePos;
                    currCache = RING_CACHES.get(cachePos);
                } else {
                    List<String> nullSpans = new LinkedList<>();
                    spanSynQueue1.put(nullSpans);
                    spanSynQueue2.put(nullSpans);
                    break;
                }
            }

            if (lastWrongTraceIds != null) {
                UPLOAD_QUEUE.put(new Pair<>(lastWrongTraceIds, batchPos));
            }

            UPLOAD_QUEUE.put(new Pair<>(null, -1));

            LOGGER.info("exit main thread");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Pair<List<String>, List<String>> splitList(List<String> list) {
        List<String> firstList = new LinkedList<>();
        for (int i = 0; i < list.size() / 2; i++) {
            firstList.add(list.get(i));
        }

        int i = 0;
        int len = list.size() / 2;
        while (i++ < len) {
            list.remove(0);
        }
        return new Pair<>(firstList, list);
    }
}
