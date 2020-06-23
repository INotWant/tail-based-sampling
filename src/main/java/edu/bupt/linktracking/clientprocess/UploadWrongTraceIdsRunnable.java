package edu.bupt.linktracking.clientprocess;

import com.alibaba.fastjson.JSON;
import edu.bupt.linktracking.Pair;
import edu.bupt.linktracking.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class UploadWrongTraceIdsRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UploadWrongTraceIdsRunnable.class.getName());

    private int remotePort;
    private final BlockingQueue<Pair<Set<String>, Integer>> uploadQueue;

    public UploadWrongTraceIdsRunnable(int remotePort, BlockingQueue<Pair<Set<String>, Integer>> uploadQueue) {
        this.remotePort = remotePort;
        this.uploadQueue = uploadQueue;
    }

    @Override
    public void run() {
        Socket clientSocket = null;
        BufferedOutputStream bos;
        try {
            clientSocket = new Socket();
            clientSocket.setTcpNoDelay(true);

            LOGGER.info("request connect!");
            clientSocket.connect(new InetSocketAddress("localhost", remotePort), 3000);
            LOGGER.info("suc to connect!");

            bos = new BufferedOutputStream(clientSocket.getOutputStream());
            while (true) {
                Pair<Set<String>, Integer> pair = uploadQueue.take();
                if (pair.first == null) {   // exit flag
                    LOGGER.warn("UPLOAD QUEUE is empty, start to send end flag!");
                    Utils.sendIntAndStringFromStream(bos, -1, "null");
                    break;
                }

                Set<String> wrongTraceIdSet = pair.first;
                int batchPos = pair.second;

                LOGGER.info("get a new pair form queue, batchPos: " + batchPos);
                Utils.sendIntAndStringFromStream(bos, batchPos, JSON.toJSONString(wrongTraceIdSet));
                LOGGER.info("suc to send lots of wrongTraceIds, batchPos: " + batchPos);
            }
            LOGGER.info("exit upload thread");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
