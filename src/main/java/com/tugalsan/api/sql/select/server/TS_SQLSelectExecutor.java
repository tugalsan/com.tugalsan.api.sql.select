package com.tugalsan.api.sql.select.server;

import com.tugalsan.api.runnable.client.*;
import com.tugalsan.api.list.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.col.typed.client.*;
import com.tugalsan.api.sql.cell.client.*;
import com.tugalsan.api.sql.order.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.group.server.*;
import com.tugalsan.api.sql.resultset.server.*;
import com.tugalsan.api.sql.sanitize.server.*;
import com.tugalsan.api.sql.where.server.*;
import com.tugalsan.api.string.client.*;
import com.tugalsan.api.time.client.*;
import com.tugalsan.api.union.client.TGS_UnionExcuse;
import com.tugalsan.api.union.client.TGS_UnionExcuseVoid;
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
        return TGS_StringUtils.toString(columnNames, ",");
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
        var sb = new StringBuilder("SELECT ").append(lineColNames).append(" FROM ").append(tableName);
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

    public TGS_UnionExcuseVoid walk(TGS_RunnableType1<TS_SQLResultSet> onEmpty, TGS_RunnableType1<TS_SQLResultSet> rs) {
        var wrap = new Object() {
            TGS_UnionExcuse<Boolean> u_empty = null;
        };
        var u_select = TS_SQLSelectStmtUtils.select(anchor, toString(), fillStmt -> {
            if (where != null) {
                where.fill(fillStmt, 0);
            }
        }, rss -> {
            d.ci("walk", () -> rss.meta.command());
            wrap.u_empty = rss.row.isEmpty();
            if (wrap.u_empty.isExcuse()) {
                return;
            }
            if (wrap.u_empty.value()) {
                if (onEmpty != null) {
                    onEmpty.run(rss);
                }
            } else {
                if (rs != null) {
                    rs.run(rss);
                }
            }
        });
        if (wrap.u_empty != null && wrap.u_empty.isExcuse()) {
            wrap.u_empty.toExcuseVoid();
        }
        return u_select;
    }

    public TGS_UnionExcuseVoid walkRows(TGS_RunnableType1<TS_SQLResultSet> onEmpty, TGS_RunnableType2<TS_SQLResultSet, Integer> rs_ri) {
        return walk(onEmpty, rs -> rs.walkRows(null, ri -> rs_ri.run(rs, ri)));
    }

    public TGS_UnionExcuseVoid walkCells(TGS_RunnableType1<TS_SQLResultSet> onEmpty, TGS_RunnableType3<TS_SQLResultSet, Integer, Integer> rs_ri_ci) {
        return walk(onEmpty, rs -> rs.walkCells(null, (ri, ci) -> rs_ri_ci.run(rs, ri, ci)));
    }

    public TGS_UnionExcuseVoid walkCols(TGS_RunnableType1<TS_SQLResultSet> onEmpty, TGS_RunnableType2<TS_SQLResultSet, Integer> rs_ci) {
        return walk(onEmpty, rs -> rs.walkCols(null, ci -> rs_ci.run(rs, ci)));
    }

    public TGS_UnionExcuse<List<TGS_SQLCellAbstract>> getRow(int rowIdx) {
        var wrap = new Object() {
            TGS_UnionExcuse<List<TGS_SQLCellAbstract>> u_rs_row_get = null;
        };
        walk(null, rs -> wrap.u_rs_row_get = rs.row.get(rowIdx));
        return wrap.u_rs_row_get;
    }

    public TGS_UnionExcuse<List<List<TGS_SQLCellAbstract>>> getRows() {
        var wrap = new Object() {
            TGS_UnionExcuse<List<TGS_SQLCellAbstract>> u_rs_row_get = null;
            List<List<TGS_SQLCellAbstract>> rows = TGS_ListUtils.of();
        };
        var u_walk = walkRows(null, (rs, ri) -> {
            if (wrap.u_rs_row_get != null && wrap.u_rs_row_get.isExcuse()) {
                return;
            }
            wrap.u_rs_row_get = rs.row.get(ri);
            if (wrap.u_rs_row_get.isExcuse()) {
                return;
            }
            wrap.rows.add(wrap.u_rs_row_get.value());
        });
        if (wrap.u_rs_row_get != null && wrap.u_rs_row_get.isExcuse()) {
            return wrap.u_rs_row_get.toExcuse();
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return TGS_UnionExcuse.of(wrap.rows);
    }

    public TGS_UnionExcuse<TGS_Time> getDate() {
        var wrap = new Object() {
            TGS_UnionExcuse<TGS_Time> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.date.get(0, 0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<TGS_Time> getTime() {
        var wrap = new Object() {
            TGS_UnionExcuse<TGS_Time> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.time.get(0, 0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<byte[]> getBlobBytes() {
        var wrap = new Object() {
            TGS_UnionExcuse<byte[]> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.bytes.get(0, 0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    @Deprecated //u can use getStr instead
    public TGS_UnionExcuse<String> getBlobStr() {
        var wrap = new Object() {
            TGS_UnionExcuse<String> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.bytesStr.get(0, 0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<String> getStr() {
        var wrap = new Object() {
            TGS_UnionExcuse<String> result = null;
            TGS_UnionExcuse<String> u_cn = null;
        };
        var u_walk = walk(null, rs -> {
            wrap.u_cn = rs.col.name(0);
            if (wrap.u_cn.isExcuse()) {
                return;
            }
            if (TGS_SQLColTypedUtils.typeBytesStr(wrap.u_cn.value())) {
                wrap.result = rs.bytesStr.get(0, 0);
                return;
            }
            if (TGS_SQLColTypedUtils.familyBytes(wrap.u_cn.value())) {
                wrap.result = TGS_UnionExcuse.of("bytes");
                return;
            }
            wrap.result = rs.str.get(0, 0);
        });
        if (wrap.u_cn != null && wrap.u_cn.isExcuse()) {
            return wrap.u_cn;
        }
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<Long> getLng() {
        var wrap = new Object() {
            TGS_UnionExcuse<Long> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.lng.get(0, 0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<List<String>> getStrLst() {
        var wrap = new Object() {
            TGS_UnionExcuse<List<String>> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.strArr.get(0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<List<List<TGS_SQLCellAbstract>>> getTbl(boolean skipBytes) {
        var wrap = new Object() {
            TGS_UnionExcuse<List<List<TGS_SQLCellAbstract>>> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.table.get(skipBytes));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<List<List<TGS_SQLCellAbstract>>> getTbl() {
        var wrap = new Object() {
            TGS_UnionExcuse<List<List<TGS_SQLCellAbstract>>> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.table.get());
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }

    public TGS_UnionExcuse<List<Long>> getLngLst() {
        var wrap = new Object() {
            TGS_UnionExcuse<List<Long>> result = null;
        };
        var u_walk = walk(null, rs -> wrap.result = rs.lngArr.get(0));
        if (wrap.result != null && wrap.result.isExcuse()) {
            return wrap.result;
        }
        if (u_walk.isExcuse()) {
            return u_walk.toExcuse();
        }
        return wrap.result;
    }
}
