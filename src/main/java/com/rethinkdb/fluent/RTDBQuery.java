package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.option.Durability;
import com.rethinkdb.fluent.types.RTFluentQueryTypes;
import com.rethinkdb.fluent.types.RTTopLevelQueryTypes;
import com.rethinkdb.proto.Q2L;

import java.util.List;

public class RTDBQuery<T> extends RTTopLevelQuery<T> {
    public RTDBQuery(RTTreeKeeper treeKeeper) {
        super(treeKeeper);
    }

    /**
     * Create table with tableName, primaryKey, Durability on a specific dataCenter.
     */
    public RTTopLevelQuery<com.rethinkdb.response.model.DDLResult> tableCreate(String tableName) {
        return tableCreate(tableName, null, null, null);
    }

    /**
     * Create table with tableName, primaryKey, Durability on a specific dataCenter.
     *
     * @param tableName  tableName (mandatory)
     * @param primaryKey primary key (leave null for default)
     * @param durability durability  (leave null for default)
     * @param datacenter datacenter  (leave null for default)
     */
    public RTTopLevelQueryTypes.DDLResult tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE_CREATE).withArgs(tableName);

        if (datacenter != null) {
            operation.withOptionalArg("datacenter", datacenter);
        }
        if (primaryKey != null) {
            operation.withOptionalArg("primary_key", primaryKey);
        }
        if (durability != null) {
            operation.withOptionalArg("durability", durability.toString());
        }

        return new RTTopLevelQueryTypes.DDLResult(treeKeeper.addData(operation));
    }

    /**
     * drop table
     *
     * @param tableName table name
     */
    public RTTopLevelQueryTypes.DDLResult tableDrop(String tableName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE_DROP).withArgs(tableName);

        return new RTTopLevelQueryTypes.DDLResult(treeKeeper.addData(operation));
    }

    /**
     * list tables
     */
    public RTTopLevelQueryTypes.StringListResult tableList() {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE_LIST);

        return new RTTopLevelQueryTypes.StringListResult(treeKeeper.addData(operation));
    }

    /**
     * Select the table to operate on
     *
     * @param tableName table name
     */
    public RTFluentQueryTypes.ObjectListResult table(String tableName) {
        // delegate to version in FluentTable (not ideal)
        return new RTFluentQuery<List>(treeKeeper).table(tableName);
    }

}
