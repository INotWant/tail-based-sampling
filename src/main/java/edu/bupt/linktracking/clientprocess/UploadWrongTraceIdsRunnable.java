package edu.bupt.linktracking.clientprocess;

import com.alibaba.fastjson.JSON;
import edu.bupt.linktracking.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

public class UploadWrongTraceIdsRunnable implements Runnable {

    private final int remotePort;
    private final SynchronousQueue<Set<String>> uploadSynQueue;


    public UploadWrongTraceIdsRunnable(int remotePort, SynchronousQueue<Set<String>> uploadSynQueue) {
        this.remotePort = remotePort;
        this.uploadSynQueue = uploadSynQueue;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());

        Socket clientSocket = null;
        BufferedOutputStream bos;
        try {
            clientSocket = new Socket();
            clientSocket.setTcpNoDelay(true);

            LOGGER.info("request connect!");
            clientSocket.connect(new InetSocketAddress("localhost", remotePort), 3000);
            LOGGER.info("suc to connect!");

            bos = new BufferedOutputStream(clientSocket.getOutputStream());
            int batchPos = -1;
            Set<String> lastWrongTraceIds = null, currWrongTraceIds;

            while (true) {
                LOGGER.info("wait a upload");
                currWrongTraceIds = uploadSynQueue.take();

                if (currWrongTraceIds == ProcessDataSecondRunnable.EXIT_FLAG_SET) {
                    if (lastWrongTraceIds != null) {
                        ++batchPos;
                        LOGGER.info("start to upload batchPos: " + batchPos);
                        Utils.sendIntAndStringFromStream(bos, batchPos, JSON.toJSONString(lastWrongTraceIds));
                        LOGGER.info("suc to upload batchPos: " + batchPos);
                    }

                    LOGGER.warn("UPLOAD QUEUE is empty, start to send end flag!");
                    Utils.sendIntAndStringFromStream(bos, -1, "null");

                    LOGGER.info("exit upload thread");
                    return;
                }

                if (lastWrongTraceIds != null) {
                    ++batchPos;
                    LOGGER.info("start to upload batchPos: " + batchPos);
                    Utils.sendIntAndStringFromStream(bos, batchPos, JSON.toJSONString(lastWrongTraceIds));
                    LOGGER.info("suc to upload batchPos: " + batchPos);
                }
                lastWrongTraceIds = currWrongTraceIds;
            }
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
