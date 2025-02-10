package com.tugalsan.api.sql.select.server;


import com.tugalsan.api.function.client.maythrow.uncheckedexceptions.TGS_FuncMTUCE_In1;
import com.tugalsan.api.sql.conn.server.TS_SQLConnColUtils;
import com.tugalsan.api.sql.where.server.TS_SQLWhereConditions;
import com.tugalsan.api.sql.where.server.TS_SQLWhereGroups;
import com.tugalsan.api.sql.where.server.TS_SQLWhereUtils;

public class TS_SQLSelectWhere {

    public TS_SQLSelectWhere(TS_SQLSelectExecutor executor) {
        this.executor = executor;
    }
    final private TS_SQLSelectExecutor executor;

    public TS_SQLSelectGroup whereGroupAnd(TGS_FuncMTUCE_In1<TS_SQLWhereGroups> gAnd) {
        executor.where = TS_SQLWhereUtils.where();
        executor.where.groupsAnd(gAnd);
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereGroupOr(TGS_FuncMTUCE_In1<TS_SQLWhereGroups> gOr) {
        executor.where = TS_SQLWhereUtils.where();
        executor.where.groupsOr(gOr);
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereConditionAnd(TGS_FuncMTUCE_In1<TS_SQLWhereConditions> cAnd) {
        whereGroupAnd(where -> where.conditionsAnd(cAnd));
        return new TS_SQLSelectGroup(executor);
    }

    public TS_SQLSelectGroup whereConditionOr(TGS_FuncMTUCE_In1<TS_SQLWhereConditions> cOr) {
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
