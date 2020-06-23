package edu.bupt.linktracking.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import edu.bupt.linktracking.Utils;
import edu.bupt.linktracking.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class AcceptWrongTraceIdsRunnable implements Runnable {

    private final int listenPort;
    private final int remotePort;

    private final BlockingQueue<TraceIdBatch> traceIdBatchQueue;

    public AcceptWrongTraceIdsRunnable(int listenPort, BlockingQueue<TraceIdBatch> traceIdBatchQueue) {
        this.listenPort = listenPort;
        if (listenPort == 8003) {
            this.remotePort = 8000;
        } else {
            this.remotePort = 8001;
        }
        this.traceIdBatchQueue = traceIdBatchQueue;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        ServerSocket serverSocket = null;
        Socket commSocket = null;
        BufferedInputStream bis;

        try {
            LOGGER.info("start to listen port: " + listenPort);
            serverSocket = new ServerSocket(listenPort);
            commSocket = serverSocket.accept();
            commSocket.setTcpNoDelay(true);

            LOGGER.info("create a connect with: " + remotePort);
            commSocket.shutdownOutput();
            bis = new BufferedInputStream(commSocket.getInputStream());

            byte[] dataBytes = new byte[1024];
            byte[] lenAndBathPosBytes = new byte[8];

            while (true) {
                Pair<Integer, String> result = new Pair<>();
                dataBytes = Utils.receiveIntAndStringFromStream(bis, lenAndBathPosBytes, dataBytes, result);
                if (dataBytes == null){
                    LOGGER.error("illegal end!");
                    return;
                }

                int batchPos = result.first;
                String content = result.second;

                if (batchPos == -1 && "null".equals(content)) {
                    TraceIdBatch traceIdBatch = new TraceIdBatch(); // exit batchPos = -1
                    traceIdBatchQueue.put(traceIdBatch);
                    break;
                }

                Set<String> wrongTraceIds = JSON.parseObject(content, new TypeReference<Set<String>>() {});

                LOGGER.info(String.format("accept WrongTraceIds, batchPos: %d, form: %s, wrongTraceId size: %d", batchPos, remotePort, wrongTraceIds.size()));

                TraceIdBatch traceIdBatch = new TraceIdBatch();
                traceIdBatch.setBatchPos(batchPos);
                traceIdBatch.getWrongTraceIds().addAll(wrongTraceIds);

                // enqueue
                traceIdBatchQueue.put(traceIdBatch);
            }
            LOGGER.info("exit accept wrongTraceId thread");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (commSocket != null) {
                    commSocket.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
