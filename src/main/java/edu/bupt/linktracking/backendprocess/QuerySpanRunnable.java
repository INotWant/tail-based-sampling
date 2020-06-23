package edu.bupt.linktracking.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import edu.bupt.linktracking.Utils;
import edu.bupt.linktracking.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class QuerySpanRunnable implements Runnable {

    private final int querySpanServicePort;
    private final SynchronousQueue<TraceIdBatch> traceIdBatchSynQueue;

    private SynchronousQueue<Map<String, List<String>>> synQueue = new SynchronousQueue<>();

    public QuerySpanRunnable(int querySpanServicePort, SynchronousQueue<TraceIdBatch> traceIdBatchSynQueue) {
        this.querySpanServicePort = querySpanServicePort;
        this.traceIdBatchSynQueue = traceIdBatchSynQueue;
    }

    public SynchronousQueue<Map<String, List<String>>> getSynQueue() {
        return synQueue;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        Socket clientSocket = null;
        BufferedOutputStream bos;
        BufferedInputStream bis;

        try {
            clientSocket = new Socket();
            clientSocket.setTcpNoDelay(true);

            LOGGER.info("request connect!");
            clientSocket.connect(new InetSocketAddress("localhost", this.querySpanServicePort), 3000);
            LOGGER.info("suc to connect!");

            bos = new BufferedOutputStream(clientSocket.getOutputStream());
            bis = new BufferedInputStream(clientSocket.getInputStream());

            byte[] dataBytes = new byte[1024];
            byte[] lenAndBathPosBytes = new byte[8];

            while (true) {
                LOGGER.info("wait traceIdBatch");
                TraceIdBatch traceIdBatch = traceIdBatchSynQueue.take();

                int batchPos = traceIdBatch.getBatchPos();
                if (batchPos == -1) {  // exit
                    break;
                }

                Set<String> wrongTraceIds = traceIdBatch.getWrongTraceIds();
                LOGGER.info("send query span request, batchPos: " + batchPos);
                Utils.sendIntAndStringFromStream(bos, batchPos, JSON.toJSONString(wrongTraceIds));

                Pair<Integer, String> result = new Pair<>();
                dataBytes = Utils.receiveIntAndStringFromStream(bis, lenAndBathPosBytes, dataBytes, result);
                if (dataBytes == null) {
                    LOGGER.error("illegal end!");
                    return;
                }

                LOGGER.info("suc to get span, batchPos: " + batchPos);
                synQueue.put(JSON.parseObject(result.second, new TypeReference<Map<String, List<String>>>() {}));
                LOGGER.info("suc to enqueue span, batchPos: " + batchPos);
            }
            LOGGER.info("exit query span thread");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
