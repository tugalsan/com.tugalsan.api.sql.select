package com.tugalsan.api.sql.select.server;

import com.tugalsan.api.function.client.maythrowexceptions.unchecked.TGS_FuncMTU_In1;
import java.util.*;

import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.string.client.TGS_StringUtils;

public class TS_SQLSelect {

    final private static TS_Log d = TS_Log.of(TS_SQLSelect.class);

    public TS_SQLSelect(TS_SQLConnAnchor anchor, CharSequence tableName) {
        executor = new TS_SQLSelectExecutor(anchor, tableName);
    }
    final private TS_SQLSelectExecutor executor;

    public TS_SQLSelectWhere columns(TGS_FuncMTU_In1<List<String>> columnNames) {
        columnNames.run(executor.columnNames);
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return columnsAll();
        }
        columns.stream().forEachOrdered(cn -> executor.columnNames.add(TGS_StringUtils.cmn().isNullOrEmpty(cn) ? TS_SQLSelectUtils.columnEmpty() : cn));
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(int... colIdxes) {
        if (colIdxes == null || colIdxes.length == 0) {
            return columnsAll();
        }
        var cnsAll = TS_SQLConnColUtils.names(executor.anchor, executor.tableName);
        Arrays.stream(colIdxes).forEachOrdered(colIdx -> {
            if (colIdx == -1) {
                executor.columnNames.add(TS_SQLSelectUtils.columnEmpty());
            } else {
                executor.columnNames.add(cnsAll.get(colIdx));
            }
        });
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(CharSequence... columns) {
        if (columns == null || columns.length == 0) {
            return columnsAll();
        }
        Arrays.stream(columns).forEachOrdered(cn -> {
            var input = TGS_StringUtils.cmn().isNullOrEmpty(cn) ? TS_SQLSelectUtils.columnEmpty() : cn.toString();
            d.ci("columns(CharSequence... columns)", input);
            executor.columnNames.add(input);
        });
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columns(String[] columns) {
        if (columns == null || columns.length == 0) {
            return columnsAll();
        }
        Arrays.stream(columns).forEachOrdered(cn -> executor.columnNames.add(TGS_StringUtils.cmn().isNullOrEmpty(cn) ? TS_SQLSelectUtils.columnEmpty() : cn));
        return new TS_SQLSelectWhere(executor);
    }

    public TS_SQLSelectWhere columnsAll() {
        return new TS_SQLSelectWhere(executor);
    }
}
