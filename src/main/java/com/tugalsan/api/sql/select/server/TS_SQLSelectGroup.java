package com.tugalsan.api.sql.select.server;


import com.tugalsan.api.function.client.maythrow.uncheckedexceptions.TGS_FuncMTUCE_In1;
import com.tugalsan.api.sql.conn.server.TS_SQLConnColUtils;
import com.tugalsan.api.sql.group.server.TS_SQLGroupUtils;
import java.util.Arrays;
import java.util.List;

public class TS_SQLSelectGroup {

    public TS_SQLSelectGroup(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    private final TS_SQLSelectExecutor executor;

    public TS_SQLSelectOrder group(int colIdx) {
        var colNames = TS_SQLConnColUtils.names(executor.anchor, executor.tableName);
        return group(colNames.get(colIdx));
    }

    public TS_SQLSelectOrder group(String columnName) {
        return group(columnNames -> {
            columnNames.add(columnName);
        });
    }

    public TS_SQLSelectOrder group(TGS_FuncMTUCE_In1<List<String>> columnNames) {
        executor.group = TS_SQLGroupUtils.group();
        columnNames.run(executor.group.columnNames);
        return new TS_SQLSelectOrder(executor);
    }

    public TS_SQLSelectOrder groupNone() {
        return new TS_SQLSelectOrder(executor);
    }

    public TS_SQLSelectOrder group(String[] columns) {
        if (columns == null || columns.length == 0) {
            return groupNone();
        }
        executor.group = TS_SQLGroupUtils.group();
        Arrays.stream(columns).forEachOrdered(cn -> executor.group.columnNames.add(cn));
        return new TS_SQLSelectOrder(executor);
    }

    public TS_SQLSelectOrder group(CharSequence... columns) {
        if (columns == null || columns.length == 0) {
            return groupNone();
        }
        executor.group = TS_SQLGroupUtils.group();
        Arrays.stream(columns).forEachOrdered(cn -> executor.group.columnNames.add(cn.toString()));
        return new TS_SQLSelectOrder(executor);
    }
}
