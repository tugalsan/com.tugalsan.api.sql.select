package com.tugalsan.api.sql.select.server;

import java.util.*;
import com.tugalsan.api.runnable.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;

public class TS_SQLSelect {

    final private static TS_Log d = TS_Log.of(TS_SQLSelect.class);

    public TS_SQLSelect(TS_SQLConnAnchor anchor, CharSequence tableName) {
        executor = new TS_SQLSelectExecutor(anchor, tableName);
    }
    final private TS_SQLSelectExecutor executor;

    public TS_SQLSelectWhere columns(TGS_RunnableType1<List<String>> columnNames) {
        columnNames.run(executor.columnNames);
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return columnsAll();
        }
        columns.stream().forEachOrdered(cn -> executor.columnNames.add(cn));
        return new TS_SQLSelectWhere(executor);
    }

    public TGS_UnionExcuse<TS_SQLSelectWhere> columns(int... colIdxes) {
        if (colIdxes == null || colIdxes.length == 0) {
            return TGS_UnionExcuse.of(columnsAll());
        }
        var u_names = TS_SQLConnColUtils.names(executor.anchor, executor.tableName);
        if (u_names.isExcuse()) {
            return u_names.toExcuse();
        }
        Arrays.stream(colIdxes).forEachOrdered(colIdx -> {
            executor.columnNames.add(u_names.value().get(colIdx));
        });
        return TGS_UnionExcuse.of(new TS_SQLSelectWhere(executor));
    }

    public TS_SQLSelectWhere columns(CharSequence... columns) {
        if (columns == null || columns.length == 0) {
            return columnsAll();
        }
        Arrays.stream(columns).forEachOrdered(cn -> {
            var input = cn.toString();
            d.ci("columns(CharSequence... columns)", input);
            executor.columnNames.add(input);
        });
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(String[] columns) {
        if (columns == null || columns.length == 0) {
            return columnsAll();
        }
        Arrays.stream(columns).forEachOrdered(cn -> executor.columnNames.add(cn));
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columnsAll() {
        return new TS_SQLSelectWhere(executor);
    }
}
