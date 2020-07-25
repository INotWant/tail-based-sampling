package edu.bupt.linktracking.backendprocess;

import com.alibaba.fastjson.JSON;
import edu.bupt.linktracking.Main;
import edu.bupt.linktracking.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;

import static edu.bupt.linktracking.Main.getDataSourcePort;
import static edu.bupt.linktracking.Main.responseDataSourceReady;

public class MainRunnable implements Runnable {

    private static BlockingQueue<TraceIdBatch> TRACEID_BATCH_QUEUE_1 = new LinkedBlockingQueue<>();
    private static BlockingQueue<TraceIdBatch> TRACEID_BATCH_QUEUE_2 = new LinkedBlockingQueue<>();

    private static SynchronousQueue<TraceIdBatch> TRACEID_BATCH_SYN_QUEUE_1 = new SynchronousQueue<>();
    private static SynchronousQueue<TraceIdBatch> TRACEID_BATCH_SYN_QUEUE_2 = new SynchronousQueue<>();

    private static Map<String, String> TRACE_CHUCKSUM_MAP = new HashMap<>();

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        ServerSocket serverSocket = null;
        try {
            // start accept wrongTraceId thread
            AcceptWrongTraceIdsRunnable acceptWrongTraceIdsRunnable1 = new AcceptWrongTraceIdsRunnable(8003, TRACEID_BATCH_QUEUE_1);
            AcceptWrongTraceIdsRunnable acceptWrongTraceIdsRunnable2 = new AcceptWrongTraceIdsRunnable(8004, TRACEID_BATCH_QUEUE_2);
            new Thread(acceptWrongTraceIdsRunnable1, "AcceptWrongTraceIdsThread1").start();
            new Thread(acceptWrongTraceIdsRunnable2, "AcceptWrongTraceIdsThread2").start();

            // start listen LISTEN_PORT (target: 1) communicate with data source (ready setParameter))
            serverSocket = new ServerSocket(Main.LISTEN_PORT);
            Socket dataSourceSocket = serverSocket.accept();
            responseDataSourceReady(dataSourceSocket);
            dataSourceSocket = serverSocket.accept();
            Main.DATA_SOURCE_PORT = getDataSourcePort(dataSourceSocket);
            if (Main.DATA_SOURCE_PORT == -1){
                LOGGER.error("fail to get data source port");
            }else {
                LOGGER.info("suc to get data source port: " + Main.DATA_SOURCE_PORT);
            }

            // start query span thread
            QuerySpanRunnable querySpanRunnable1 = new QuerySpanRunnable(8000, TRACEID_BATCH_SYN_QUEUE_1);
            QuerySpanRunnable querySpanRunnable2 = new QuerySpanRunnable(8001, TRACEID_BATCH_SYN_QUEUE_2);
            new Thread(querySpanRunnable1, "QuerySpanThread1").start();
            new Thread(querySpanRunnable2, "QuerySpanThread2").start();

            // bridge
            TraceIdBatch traceIdBatch1;
            TraceIdBatch traceIdBatch2;
            TraceIdBatch traceIdBatch;
            TraceIdBatch nullTraceIdBatch = new TraceIdBatch();
            SynchronousQueue<Map<String, List<String>>> synQueue1 = querySpanRunnable1.getSynQueue();
            SynchronousQueue<Map<String, List<String>>> synQueue2 = querySpanRunnable2.getSynQueue();
            while (true) {
                traceIdBatch1 = TRACEID_BATCH_QUEUE_1.take();
                traceIdBatch2 = TRACEID_BATCH_QUEUE_2.take();

                int batchPos1 = traceIdBatch1.getBatchPos();
                int batchPos2 = traceIdBatch2.getBatchPos();
                if (batchPos1 == -1 || batchPos2 == -1) {
                    TRACEID_BATCH_SYN_QUEUE_1.put(nullTraceIdBatch);
                    TRACEID_BATCH_SYN_QUEUE_2.put(nullTraceIdBatch);
                    break;
                }

                if (batchPos1 != batchPos2) {
                    LOGGER.warn("batchPos1 not equal batchPos2");
                    continue;
                }

                traceIdBatch = new TraceIdBatch();
                traceIdBatch.setBatchPos(batchPos1);
                traceIdBatch.getWrongTraceIds().addAll(traceIdBatch1.getWrongTraceIds());
                traceIdBatch.getWrongTraceIds().addAll(traceIdBatch2.getWrongTraceIds());

                TRACEID_BATCH_SYN_QUEUE_1.put(traceIdBatch);
                TRACEID_BATCH_SYN_QUEUE_2.put(traceIdBatch);

                Map<String, List<String>> resultMap1 = synQueue1.take();
                Map<String, List<String>> resultMap2 = synQueue2.take();
                computeCheckSum(resultMap1, resultMap2);

                LOGGER.info("suc to compute check sum, batchPos: " + batchPos1);
            }

            // send check sum
            while (!sendCheckSum()){
                LOGGER.warn("fail to send check sum");
            }

            LOGGER.info("suc to send check sum, exit main thread");
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

    private static void computeCheckSum(Map<String, List<String>> resultMap1, Map<String, List<String>> resultMap2) {
        for (Map.Entry<String, List<String>> entry : resultMap2.entrySet()) {
            String key2 = entry.getKey();
            List<String> value2 = entry.getValue();
            List<String> value1 = resultMap1.get(key2);
            if (value1 == null) {
                resultMap1.put(key2, value2);
            } else {
                value1.addAll(value2);
            }
        }
        for (Map.Entry<String, List<String>> entry : resultMap1.entrySet()) {
            String traceId = entry.getKey();
            Set<String> spanSet = new HashSet<>(entry.getValue());
            String spans = spanSet.stream().
                    sorted(Comparator.comparing(MainRunnable::getStartTime)).
                    collect(Collectors.joining("\n"));
            spans = spans + "\n";
            TRACE_CHUCKSUM_MAP.put(traceId, Utils.MD5(spans));
        }
    }

    private static long getStartTime(String span) {
        if (span != null) {
            String[] cols = span.split("\\|");
            if (cols.length > 8) {
                return Utils.toLong(cols[1], -1);
            }
        }
        return -1;
    }

    private static boolean sendCheckSum() {
        String path = String.format("http://localhost:%s/api/finished", Main.DATA_SOURCE_PORT);
        String param = "result=" + URLEncoder.encode(JSON.toJSONString(TRACE_CHUCKSUM_MAP));
        try {
            URL url = new URL(path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
            httpConnection.setRequestMethod("POST");
            httpConnection.setDoOutput(true);

            DataOutputStream dos = new DataOutputStream(httpConnection.getOutputStream());
            dos.writeBytes(param);
            dos.flush();
            dos.close();

            int resultCode = httpConnection.getResponseCode();
            return HttpURLConnection.HTTP_OK == resultCode;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
