package com.tugalsan.api.sql.select.server;

import java.sql.*;
import java.util.*;
import com.tugalsan.api.executable.client.*;
import com.tugalsan.api.log.server.*;
import com.tugalsan.api.sql.conn.server.*;
import com.tugalsan.api.sql.resultset.server.*;

public class TS_SQLSelectStmtUtils {

    final private static TS_Log d = TS_Log.of(TS_SQLSelectStmtUtils.class);

    //WARNING: CHECK TO SEE IF SQL IS SAFE!
    @Deprecated
    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, TGS_ExecutableType1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, new String[0], new Object[0], rs);
    }

    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, String[] colNames, Object[] params, TGS_ExecutableType1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, fillStmt -> TS_SQLConnStmtUtils.fill(fillStmt, colNames, params, 0), rs);
    }

    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, List<String> colNames, List params, TGS_ExecutableType1<TS_SQLResultSet> rs) {
        select(anchor, sqlStmt, fillStmt -> TS_SQLConnStmtUtils.fill(fillStmt, colNames, params, 0), rs);
    }

    public static void select(TS_SQLConnAnchor anchor, CharSequence sqlStmt, TGS_ExecutableType1<PreparedStatement> fillStmt, TGS_ExecutableType1<TS_SQLResultSet> rs) {
        TS_SQLConnWalkUtils.query(anchor, sqlStmt, fillStmt, rs);
    }
}
