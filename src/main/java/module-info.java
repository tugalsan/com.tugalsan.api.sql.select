module com.tugalsan.api.sql.select {
    requires java.sql;
    
    requires com.tugalsan.api.log;
    requires com.tugalsan.api.list;
    requires com.tugalsan.api.tuple;
    requires com.tugalsan.api.thread;
    requires com.tugalsan.api.string;
    requires com.tugalsan.api.time;
    requires com.tugalsan.api.function;
    requires com.tugalsan.api.sql.conn;
    requires com.tugalsan.api.sql.cell;
    requires com.tugalsan.api.sql.order;
    requires com.tugalsan.api.sql.col.typed;
    requires com.tugalsan.api.sql.resultset;
    requires com.tugalsan.api.sql.sanitize;
    requires com.tugalsan.api.sql.where;
    requires com.tugalsan.api.sql.group;
    exports com.tugalsan.api.sql.select.server;
}
