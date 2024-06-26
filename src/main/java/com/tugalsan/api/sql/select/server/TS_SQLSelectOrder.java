package com.tugalsan.api.sql.select.server;


import com.tugalsan.api.callable.client.TGS_CallableType1_Run;
import com.tugalsan.api.sql.conn.server.TS_SQLConnColUtils;
import com.tugalsan.api.sql.order.server.TS_SQLOrderUtils;
import java.util.Arrays;
import java.util.List;

public class TS_SQLSelectOrder {

    public TS_SQLSelectOrder(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    private final TS_SQLSelectExecutor executor;

    public TS_SQLSelectRowIdxOffset orderAsc(String[] columns) {
        if (columns == null || columns.length == 0) {
            return orderNone();
        }
        executor.order = TS_SQLOrderUtils.asc();
        Arrays.stream(columns).forEachOrdered(cn -> executor.order.columnNames.add(cn));
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderAsc(CharSequence... columns) {
        if (columns == null || columns.length == 0) {
            return orderNone();
        }
        executor.order = TS_SQLOrderUtils.asc();
        Arrays.stream(columns).forEachOrdered(cn -> executor.order.columnNames.add(cn.toString()));
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderDesc(String[] columns) {
        if (columns == null || columns.length == 0) {
            return orderNone();
        }
        executor.order = TS_SQLOrderUtils.desc();
        Arrays.stream(columns).forEachOrdered(cn -> executor.order.columnNames.add(cn));
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderDesc(CharSequence... columns) {
        if (columns == null || columns.length == 0) {
            return orderNone();
        }
        executor.order = TS_SQLOrderUtils.desc();
        Arrays.stream(columns).forEachOrdered(cn -> executor.order.columnNames.add(cn.toString()));
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderAsc(int colIdx) {
        var colNames = TS_SQLConnColUtils.names(executor.anchor, executor.tableName);
        if (colIdx < 0 || colIdx >= colNames.size()) {
            return orderNone();
        }
        return orderAsc(colNames.get(colIdx));
    }

    public TS_SQLSelectRowIdxOffset orderDesc(int colIdx) {
        var colNames = TS_SQLConnColUtils.names(executor.anchor, executor.tableName);
        if (colIdx < 0 || colIdx >= colNames.size()) {
            return orderNone();
        }
        return orderDesc(colNames.get(colIdx));
    }

    public TS_SQLSelectRowIdxOffset orderAsc(String columnName) {
        return orderAsc(columnNames -> {
            columnNames.add(columnName);
        });
    }

    public TS_SQLSelectRowIdxOffset orderDesc(String columnName) {
        return orderDesc(columnNames -> {
            columnNames.add(columnName);
        });
    }

    public TS_SQLSelectRowIdxOffset orderAsc(TGS_CallableType1_Run<List<String>> columnNames) {
        executor.order = TS_SQLOrderUtils.asc();
        columnNames.run(executor.order.columnNames);
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderDesc(TGS_CallableType1_Run<List<String>> columnNames) {
        executor.order = TS_SQLOrderUtils.desc();
        columnNames.run(executor.order.columnNames);
        return new TS_SQLSelectRowIdxOffset(executor);
    }

    public TS_SQLSelectRowIdxOffset orderNone() {
        return new TS_SQLSelectRowIdxOffset(executor);
    }
}
