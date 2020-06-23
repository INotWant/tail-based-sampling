package edu.bupt.linktracking.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import edu.bupt.linktracking.Pair;
import edu.bupt.linktracking.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class QuerySpanServiceRunnable implements Runnable {

    private final Socket backendSocket;

    private final List<Map<String, List<String>>> ringCaches;
    private final Lock lockForRingCaches;
    private final Condition conditionForRingCaches;


    public QuerySpanServiceRunnable(Socket backendSocket, List<Map<String, List<String>>> ringCaches, Lock lockForRingCaches, Condition conditionForRingCaches) {
        this.backendSocket = backendSocket;
        this.ringCaches = ringCaches;
        this.lockForRingCaches = lockForRingCaches;
        this.conditionForRingCaches = conditionForRingCaches;
    }

    @Override
    public void run() {
        final Logger LOGGER = LoggerFactory.getLogger(Thread.currentThread().getName());
        try {
            this.backendSocket.setTcpNoDelay(true);

            BufferedInputStream bis = new BufferedInputStream(this.backendSocket.getInputStream());
            BufferedOutputStream bos = new BufferedOutputStream(this.backendSocket.getOutputStream());

            byte[] dataBytes = new byte[1024];
            byte[] lenAndBathPosBytes = new byte[8];
            while (true) {
                Pair<Integer, String> result = new Pair<>();
                dataBytes = Utils.receiveIntAndStringFromStream(bis, lenAndBathPosBytes, dataBytes, result);
                if (dataBytes == null) {
                    LOGGER.error("illegal end!");
                    return;
                }
                if (result.first == null || result.second == null){
                    LOGGER.info("socket closed by peer, exit thread");
                    return;
                }

                int batchPos = result.first;
                String wrongTraceIdsStr = result.second;

                LOGGER.info("get a request, batchPos: " + batchPos);

                String responseStr = getWrongTracing(wrongTraceIdsStr, batchPos);
                Utils.sendIntAndStringFromStream(bos, batchPos, responseStr);

                LOGGER.info("suc to repose, batchPos: " + batchPos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                backendSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getWrongTracing(String wrongTraceIdsStr, int batchPos) {
        int total = ringCaches.size();
        int curr = batchPos % total;
        int pre = (curr - 1) == -1 ? total - 1 : curr - 1;
        int next = (curr + 1) == total ? 0 : curr + 1;

        Set<String> traceIdList = JSON.parseObject(wrongTraceIdsStr, new TypeReference<Set<String>>() {});

        Map<String, List<String>> wrongTraceIdMap;
        if (traceIdList != null) {
            wrongTraceIdMap = getWrongTracesAll(traceIdList, pre, curr, next);
        } else {
            wrongTraceIdMap = new HashMap<>();
        }

        if (batchPos > 0) {
            Map<String, List<String>> preTraceIdMap = ringCaches.get(pre);
            if (preTraceIdMap.size() > 0) {
                preTraceIdMap.clear();
                lockForRingCaches.lock();
                try {
                    conditionForRingCaches.signalAll();
                } finally {
                    lockForRingCaches.unlock();
                }
            }
        }
        return JSON.toJSONString(wrongTraceIdMap);
    }

    private Map<String, List<String>> getWrongTracesAll(Iterable<String> wrongTraceIds, int pre, int curr, int next) {
        Map<String, List<String>> wrongTraceIdMap = new HashMap<>();
        getWrongTracesWithBatch(wrongTraceIds, pre, wrongTraceIdMap);
        getWrongTracesWithBatch(wrongTraceIds, curr, wrongTraceIdMap);
        getWrongTracesWithBatch(wrongTraceIds, next, wrongTraceIdMap);
        return wrongTraceIdMap;
    }

    private void getWrongTracesWithBatch(Iterable<String> wrongTraceIds, int pos, Map<String, List<String>> wrongTraceIdMap) {
        Map<String, List<String>> traceMap = ringCaches.get(pos);
        for (String wrongTraceId : wrongTraceIds) {
            List<String> spanList = traceMap.get(wrongTraceId);
            if (spanList != null && spanList.size() > 0) {
                // one trace may cross to batch (e.g batch size 20000, span1 in line 19999, span2 in line 20001)
                List<String> existSpanList = wrongTraceIdMap.get(wrongTraceId);
                if (existSpanList != null)
                    existSpanList.addAll(spanList);
                else
                    wrongTraceIdMap.put(wrongTraceId, spanList);
            }
        }
    }
}
