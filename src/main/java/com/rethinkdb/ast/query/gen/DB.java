package com.rethinkdb.ast.query.gen;

import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.ast.helper.OptionalArguments;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.Durability;
import com.rethinkdb.proto.Q2L;

import java.util.List;

// extends RqlTopLevelQuery
public class DB extends RqlQuery {

    public DB(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        super(Q2L.Term.TermType.DB, args, optionalArgs);
    }

    public TableCreate tableCreate(String tableName) {
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
    public TableCreate tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
        OptionalArguments optionalArguments = new OptionalArguments()
                .with("datacenter", datacenter)
                .with("primary_key", primaryKey)
                .with("durability", durability == null ? null : durability.toString());

        return new TableCreate(this, new Arguments(tableName), optionalArguments);
    }

    /**
     * drop table
     *
     * @param tableName table name
     */
    public TableDrop tableDrop(String tableName) {
        return new TableDrop(this, new Arguments(tableName), null);
    }

    /**
     * list tables
     */
    public TableList tableList() {
        return new TableList(this, null, null);

    }

}
        