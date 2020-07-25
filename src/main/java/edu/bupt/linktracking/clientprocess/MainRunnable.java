package edu.bupt.linktracking.clientprocess;

import edu.bupt.linktracking.Constants;
import edu.bupt.linktracking.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    // size of the ring cache
    private static int CACHE_TOTAL = 12;

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

            SynchronousQueue<String> dataBlockSynQueue = new SynchronousQueue<>();
            SynchronousQueue<List<String>> spansSynQueue = new SynchronousQueue<>();
            SynchronousQueue<Set<String>> uploadSynQueue = new SynchronousQueue<>();


            ProcessDataFirstRunnable processDataFirstRunnable = new ProcessDataFirstRunnable(dataBlockSynQueue, spansSynQueue);
            new Thread(processDataFirstRunnable, "ProcessDataThread1").start();

            ProcessDataSecondRunnable processDataSecondRunnable = new ProcessDataSecondRunnable(RING_CACHES, spansSynQueue, uploadSynQueue,
                    LOCK_FOR_RING_CACHES, CONDITION_FOR_RING_CACHES);
            new Thread(processDataSecondRunnable, "ProcessDataThread2").start();

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
            new Thread(new UploadWrongTraceIdsRunnable(uploadRemotePort, uploadSynQueue), "UploadWrongTraceIdsThread").start();

            // start read data thread (need DATA_SOURCE_PORT)
            String dataPath = getDataPath();
            long rangeSize = 4 * Constants.MB_SIZE;
            long rangeStep = 4 * Constants.MB_SIZE;
            long rangeValueStart = 0;
            ReadDataRunnable readDataRunnable = new ReadDataRunnable(dataPath, rangeSize, rangeStep, rangeValueStart, dataBlockSynQueue);
            new Thread(readDataRunnable, "ReadDataThread").start();

            // wait backend connection
            Socket backendSocket = serverSocket.accept();
            new Thread(new QuerySpanServiceRunnable(backendSocket, RING_CACHES, LOCK_FOR_RING_CACHES, CONDITION_FOR_RING_CACHES),
                    "QuerySpanServiceThread").start();

            LOGGER.info("exit main thread");
        } catch (IOException e) {
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
}
