package edu.bupt.linktracking.backendprocess;

import java.util.HashSet;
import java.util.Set;

public class TraceIdBatch {
    private int batchPos = -1;
    private Set<String> wrongTraceIds = new HashSet<>();


    public int getBatchPos() {
        return batchPos;
    }

    public void setBatchPos(int batchPos) {
        this.batchPos = batchPos;
    }

    public Set<String> getWrongTraceIds() {
        return wrongTraceIds;
    }

}
