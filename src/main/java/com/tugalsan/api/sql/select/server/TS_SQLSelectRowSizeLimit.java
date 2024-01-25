package com.tugalsan.api.sql.select.server;

public class TS_SQLSelectRowSizeLimit {

    public TS_SQLSelectRowSizeLimit(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    private final TS_SQLSelectExecutor executor;

    public TS_SQLSelectExecutor rowSizeLimitNone() {
        return executor;
    }

    public TS_SQLSelectExecutor rowSizeLimit(int rowSizeLimit) {
        executor.rowSizeLimit = rowSizeLimit;
        return executor;
    }
}
