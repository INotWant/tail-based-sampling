package edu.bupt.linktracking.clientprocess;

import edu.bupt.linktracking.Constants;
import edu.bupt.linktracking.Main;
import edu.bupt.linktracking.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static BlockingQueue<List<String>> SPAN_QUEUE = new LinkedBlockingQueue<>(8);

    private static List<Map<String, List<String>>> RING_CACHES = new ArrayList<>();
    private static Lock LOCK_FOR_RING_CACHES = new ReentrantLock();
    private static Condition CONDITION_FOR_RING_CACHES = LOCK_FOR_RING_CACHES.newCondition();

    private static BlockingQueue<Pair<Set<String>, Integer>> UPLOAD_QUEUE = new LinkedBlockingQueue<>();

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

            // start process data thread
            ProcessDataRunnable processDataRunnable = new ProcessDataRunnable(SPAN_QUEUE, RING_CACHES, UPLOAD_QUEUE,
                    LOCK_FOR_RING_CACHES, CONDITION_FOR_RING_CACHES);
            Thread processDataThread = new Thread(processDataRunnable, "ProcessDataThread");
            processDataThread.start();

            // start listen LISTEN_PORT (target: 1) communicate with data source; 2) communicate with backend for uploading span)
            serverSocket = new ServerSocket(Main.LISTEN_PORT);
            Socket dataSourceSocket = serverSocket.accept();
            responseDataSourceReady(dataSourceSocket);
            dataSourceSocket = serverSocket.accept();
            Main.DATA_SOURCE_PORT = getDataSourcePort(dataSourceSocket);
            if (Main.DATA_SOURCE_PORT == -1){
                LOGGER.error("fail to get data source port");
            }else {
                LOGGER.error("suc to get data source port: " + Main.DATA_SOURCE_PORT);
            }

            // start upload thread
            int uploadRemotePort = Main.LISTEN_PORT == 8000 ? 8003 : 8004;
            new Thread(new UploadWrongTraceIdsRunnable(uploadRemotePort, UPLOAD_QUEUE), "UploadWrongTraceIdsThread").start();

            // start read data thread (need DATA_SOURCE_PORT)
            String dataPath = getDataPath();
            long rangeSize = 4 * Constants.MB_SIZE;
            long rangeStep = 4 * Constants.MB_SIZE;
            long rangeValueStart = 0;
            ReadDataRunnable readDataRunnable1 = new ReadDataRunnable(dataPath, rangeSize, rangeStep, rangeValueStart);
            new Thread(readDataRunnable1, "ReadDataThread1").start();

            // wait backend connection
            Socket backendSocket = serverSocket.accept();
            new Thread(new QuerySpanServiceRunnable(backendSocket, RING_CACHES, LOCK_FOR_RING_CACHES, CONDITION_FOR_RING_CACHES),
                    "QuerySpanServiceThread").start();

            // bridge read thread & process thread
            SynchronousQueue<List<String>> queue1 = readDataRunnable1.getSynQueue();
            while (true) {
                List<String> dataBlockStr1 = queue1.take();
                if (dataBlockStr1.size() > 0) {
                    SPAN_QUEUE.put(dataBlockStr1);
                } else {
                    SPAN_QUEUE.put(new ArrayList<>());
                    break;
                }
            }

            processDataThread.join();
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
}
