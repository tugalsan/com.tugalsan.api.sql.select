package com.tugalsan.api.sql.select.server;

import module com.tugalsan.api.function;
import module com.tugalsan.api.sql.conn;
import module com.tugalsan.api.sql.where;

public class TS_SQLSelectWhere {

    public TS_SQLSelectWhere(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    final private TS_SQLSelectExecutor executor;

    public TS_SQLSelectGroup whereGroupAnd(TGS_FuncMTU_In1<TS_SQLWhereGroups> gAnd) {
        executor.where = TS_SQLWhereUtils.where();
        executor.where.groupsAnd(gAnd);
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereGroupOr(TGS_FuncMTU_In1<TS_SQLWhereGroups> gOr) {
        executor.where = TS_SQLWhereUtils.where();
        executor.where.groupsOr(gOr);
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereConditionAnd(TGS_FuncMTU_In1<TS_SQLWhereConditionsAnd> cAnd) {
        whereGroupAnd(where -> where.conditionsAnd(cAnd));
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereConditionOr(TGS_FuncMTU_In1<TS_SQLWhereConditionsOr> cOr) {
        whereGroupOr(where -> where.conditionsOr(cOr));
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectExecutor whereFirstColumnAsId(long id) {
        return whereConditionAnd(conditions -> {
            conditions.lngEq(
                    TS_SQLConnColUtils.names(executor.anchor, executor.tableName).get(0),
                    id
            );
        }).groupNone().orderNone().rowIdxOffsetNone().rowSizeLimitNone();
    }

    public TS_SQLSelectGroup whereConditionNone() {
        return new TS_SQLSelectGroup(executor);
    }
}
