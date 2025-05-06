package com.tugalsan.api.sql.select.server;

import com.tugalsan.api.function.client.maythrowexceptions.unchecked.TGS_FuncMTU_In1;
import com.tugalsan.api.function.client.maythrowexceptions.unchecked.TGS_FuncMTU_In2;
import com.tugalsan.api.function.client.maythrowexceptions.unchecked.TGS_FuncMTU_In3;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.tuple.client.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.cell.client.*;
import com.tugalsan.api.sql.order.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.group.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.where.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.thread.server.sync.TS_ThreadSyncTrigger;
import com.tugalsan.api.time.client.*;
import java.util.*;

public class TS_SQLSelectExecutor {

    final private static TS_Log d = TS_Log.of(TS_SQLSelectExecutor.class);

    public TS_SQLSelectExecutor(TS_SQLConnAnchor anchor, CharSequence tableName) {
        this.anchor = anchor;
        this.tableName = tableName;
    }
    final public TS_SQLConnAnchor anchor;
    final public CharSequence tableName;

    public List<String> columnNames = TGS_ListUtils.of();
    public TS_SQLWhere where = null;
    public TS_SQLOrder order = null;
    public TS_SQLGroup group = null;
    public long rowIdxOffset = 0L;
    public Integer rowSizeLimit = null;

    private String columnNames_toString() {
        d.ci("columnNames_toString", columnNames);
        if (columnNames.isEmpty()) {
            return "*";
        }
        TS_SQLSanitizeUtils.sanitize(columnNames);
        return TGS_StringUtils.cmn().toString(columnNames, ",");
    }

    @Override
    public String toString() {
        var lineColNames = columnNames_toString();
        d.ci("toString", "lineColNames", lineColNames);
        d.ci("toString", "tableName", tableName);
        d.ci("toString", "where", where);
        d.ci("toString", "group", group);
        d.ci("toString", "order", order);
        d.ci("toString", "rowIdxOffset", rowIdxOffset);
        d.ci("toString", "rowSizeLimit", rowSizeLimit);
        var sb = new StringBuilder(anchor.tagSelectAndSpace()).append(lineColNames).append(" FROM ").append(tableName);
        if (where != null) {
            sb.append(" ").append(where);
        }
        if (group != null) {
            sb.append(" ").append(group);
        }
        if (order != null) {
            sb.append(" ").append(order);
        }
        if (rowIdxOffset != 0L) {
            sb.append(" OFFSET ").append(rowIdxOffset);
        }
        if (rowSizeLimit != null) {
            sb.append(" LIMIT ").append(rowSizeLimit);
        }
        var stmt = sb.toString();
        d.ci("toString", stmt);
        return stmt;
    }

    public void walk(TGS_FuncMTU_In1<TS_SQLResultSet> onEmpty, TGS_FuncMTU_In1<TS_SQLResultSet> rs) {
        TS_SQLSelectStmtUtils.select(anchor, toString(), fillStmt -> {
            if (where != null) {
                where.fill(fillStmt, 0);
            }
        }, rss -> {
            d.ci("walk", () -> rss.meta.command());
            if (rss.row.isEmpty()) {
                if (onEmpty != null) {
                    onEmpty.run(rss);
                }
            } else {
                if (rs != null) {
                    rs.run(rss);
                }
            }
        });
    }

    public void walkRows(TGS_FuncMTU_In1<TS_SQLResultSet> onEmpty, TGS_FuncMTU_In2<TS_SQLResultSet, Integer> rs_ri) {
        walk(onEmpty, rs -> rs.walkRows(null, ri -> rs_ri.run(rs, ri)));
    }

    public void walkCells(TGS_FuncMTU_In1<TS_SQLResultSet> onEmpty, TGS_FuncMTU_In3<TS_SQLResultSet, Integer, Integer> rs_ri_ci) {
        walk(onEmpty, rs -> rs.walkCells(null, (ri, ci) -> rs_ri_ci.run(rs, ri, ci)));
    }

    public void walkCols(TGS_FuncMTU_In1<TS_SQLResultSet> onEmpty, TGS_FuncMTU_In2<TS_SQLResultSet, Integer> rs_ci) {
        walk(onEmpty, rs -> rs.walkCols(null, ci -> rs_ci.run(rs, ci)));
    }

    public List<TGS_SQLCellAbstract> getRow(int rowIdx) {
        TGS_Tuple1<List<TGS_SQLCellAbstract>> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.row.get(rowIdx));
        return pack.value0;
    }

    public List<List<TGS_SQLCellAbstract>> getRows() {
        List<List<TGS_SQLCellAbstract>> rows = TGS_ListUtils.of();
        walkRows(null, (rs, ri) -> rows.add(rs.row.get(ri)));
        return rows;
    }

    public TGS_Time getDate() {
        TGS_Tuple1<TGS_Time> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.date.get(0, 0));
        return pack.value0;
    }

    public TGS_Time getTime() {
        TGS_Tuple1<TGS_Time> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.time.get(0, 0));
        return pack.value0;
    }

    public byte[] getBlobBytes() {
        TGS_Tuple1<byte[]> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.bytes.get(0, 0));
        return pack.value0;
    }

    @Deprecated //u can use getStr instead
    public String getBlobStr() {
        TGS_Tuple1<String> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.bytesStr.get(0, 0));
        return pack.value0;
    }

    public String getStr() {
        TGS_Tuple1<String> pack = new TGS_Tuple1();
        walk(null, rs -> {
            var cn = rs.col.name(0);
            if (TGS_SQLColTypedUtils.typeBytesStr(cn)) {
                pack.value0 = rs.bytesStr.get(0, 0);
                return;
            }
            if (TGS_SQLColTypedUtils.familyBytes(cn)) {
                pack.value0 = "bytes";
                return;
            }
            pack.value0 = rs.str.get(0, 0);
        });
        return pack.value0;
    }

    public Long getLng() {
        TGS_Tuple1<Long> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.lng.get(0, 0));
        return pack.value0;
    }

    public List<String> getStrLst() {
        TGS_Tuple1<List<String>> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.strArr.get(0));
        return pack.value0 == null ? TGS_ListUtils.of() : pack.value0;
    }

    public List<List<TGS_SQLCellAbstract>> getTbl(TS_ThreadSyncTrigger servletKillTrigger, boolean skipBytes) {
        TGS_Tuple1<List<List<TGS_SQLCellAbstract>>> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.table.get(servletKillTrigger, skipBytes));
        return pack.value0 == null ? TGS_ListUtils.of() : pack.value0;
    }

    public List<List<TGS_SQLCellAbstract>> getTbl(TS_ThreadSyncTrigger servletKillTrigger) {
        TGS_Tuple1<List<List<TGS_SQLCellAbstract>>> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.table.get(servletKillTrigger));
        return pack.value0 == null ? TGS_ListUtils.of() : pack.value0;
    }

    public List<Long> getLngLst() {
        TGS_Tuple1<List<Long>> pack = new TGS_Tuple1();
        walk(null, rs -> pack.value0 = rs.lngArr.get(0));
        return pack.value0 == null ? TGS_ListUtils.of() : pack.value0;
    }
}
