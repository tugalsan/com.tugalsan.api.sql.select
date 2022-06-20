package com.tugalsan.api.sql.select.server;

public class TS_SQLSelectRowIdxOffset {

    public TS_SQLSelectRowIdxOffset(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    private TS_SQLSelectExecutor executor;

    public TS_SQLSelectRowSizeLimit rowIdxOffsetNone() {
        return new TS_SQLSelectRowSizeLimit(executor);
    }

    public TS_SQLSelectRowSizeLimit rowIdxOffset(int rowIdxOffset) {
        executor.rowIdxOffset = rowIdxOffset;
        return new TS_SQLSelectRowSizeLimit(executor);
    }
}
