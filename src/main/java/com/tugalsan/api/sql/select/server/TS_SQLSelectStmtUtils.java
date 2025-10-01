package com.tugalsan.api.sql.select.server;

import module com.tugalsan.api.function;
import module com.tugalsan.api.sql.conn;
import module com.tugalsan.api.sql.resultset;
import module java.sql;
import java.util.*;

public class TS_SQLSelectStmtUtils {

    private TS_SQLSelectStmtUtils() {

    }

//    final private static TS_Log d = TS_Log.of(TS_SQLSelectStmtUtils.class);
    @Deprecated //WARNING: CHECK TO SEE IF SQL IS SAFE!
    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, TGS_FuncMTU_In1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, new String[0], new Object[0], rs);
    }

    @Deprecated //WARNING: CHECK TO SEE IF SQL IS SAFE!
    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, String[] colNames, Object[] params, TGS_FuncMTU_In1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, fillStmt -> TS_SQLConnStmtUtils.fill(fillStmt, colNames, params, 0), rs);
    }

    @Deprecated //WARNING: CHECK TO SEE IF SQL IS SAFE!
    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, List<String> colNames, List params, TGS_FuncMTU_In1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, fillStmt -> TS_SQLConnStmtUtils.fill(fillStmt, colNames, params, 0), rs);
    }

    @Deprecated //WARNING: CHECK TO SEE IF SQL IS SAFE!
    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, TGS_FuncMTU_In1<PreparedStatement> fillStmt, TGS_FuncMTU_In1<TS_SQLResultSet> rs) {
        TS_SQLConnWalkUtils.query(anchor, sqlStmt, fillStmt, rs);
    }
}
