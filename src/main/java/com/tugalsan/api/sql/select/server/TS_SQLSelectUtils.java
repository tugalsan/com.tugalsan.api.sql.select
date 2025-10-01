package com.tugalsan.api.sql.select.server;

import module com.tugalsan.api.sql.conn;

public class TS_SQLSelectUtils {

    private TS_SQLSelectUtils() {

    }

    public static TS_SQLSelect select(TS_SQLConnAnchor anchor, CharSequence tableName) {
        return new TS_SQLSelect(anchor, tableName);
    }

    public static String columnEmpty() {
        return "''";
    }

//    public static void test() {
//        TS_SQLSelectUtils.select(null, "tn").columns(columnNames -> {
//            columnNames.add("ali gel");
//        }).whereConditionAnd(conditions -> {
//            conditions.lngEq("", 0);
//        }).groupNone().orderNone().rowIdxOffsetNone().rowSizeLimitNone().walk(null, rs -> {
//        });
//    }
}
