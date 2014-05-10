package com.rethinkdb.query;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResult;

public class RQLQueryBuilder {
    public enum Durability { hard, soft };

    private String dbName = "test";
    private String tableName = "test";

    public RQLQueryBuilder db(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public RQLQueryBuilder table(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public TerminatingQuery dbCreate(String dbName) {
        return new TerminatingQuery(
                DBResult.ExpectedDBResult.Generic,
                new QueryInformation()
                    .ofTermType(Q2L.Term.TermType.DB_CREATE)
                    .withArgs(TermBuilder.datumTerm(dbName))
        );
    }

    public TerminatingQuery dbDrop(String dbName) {
        return new TerminatingQuery(
                DBResult.ExpectedDBResult.Generic,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_DROP)
                        .withArgs(TermBuilder.datumTerm(dbName))
        );
    }

    public TerminatingQuery dbList() {
        return new TerminatingQuery(
                DBResult.ExpectedDBResult.StringList,
                new QueryInformation().ofTermType(Q2L.Term.TermType.DB_LIST)
        );
    }

    public TerminatingQuery tableCreate(String tableName) {
        return tableCreate(tableName, null, null, null);
    }

    public TerminatingQuery tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.TABLE_CREATE)
                .withArgs(TermBuilder.datumTerm(tableName));

        if (primaryKey != null) {
            information.withOptArg("primary_key", TermBuilder.datumTerm(tableName));
        }
        if (datacenter != null) {
            information.withOptArg("datacenter", TermBuilder.datumTerm(tableName));
        }
        if (durability != null) {
            information.withOptArg("durability", TermBuilder.datumTerm(durability.toString()));
        }

        return new TerminatingQuery(
                DBResult.ExpectedDBResult.Generic,
                information
        );
    }

    public TerminatingQuery tableDrop(String tableName) {
        return new TerminatingQuery(
                DBResult.ExpectedDBResult.Generic,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_DROP)
                        .withArgs(TermBuilder.datumTerm(tableName))
        );
    }

    public TerminatingQuery tableList() {
        return new TerminatingQuery(
                DBResult.ExpectedDBResult.StringList,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_LIST)
                        .withArgs(TermBuilder.datumTerm(tableName))
        );
    }

    public TerminatingQuery insert(DBObject object) { return insert(object, null, false, false); }

    public TerminatingQuery insert(DBObject object, Durability durability, boolean returnVals, boolean upsert) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.INSERT)
                .withArgs(
                        new TermBuilder()
                                .ofType(Q2L.Term.TermType.TABLE)
                                .addArg(TermBuilder.datumTerm(tableName))
                                .build(),

                        TermBuilder.createDatumTerm(object)
                );

        if (returnVals) {
            information.withOptArg("return_vals", TermBuilder.datumTerm(true));
        }
        if (upsert) {
            information.withOptArg("upsert", TermBuilder.datumTerm(true));
        }
        if (durability != null) {
            information.withOptArg("durability", TermBuilder.datumTerm(durability.toString()));
        }

        return new TerminatingQuery(
                DBResult.ExpectedDBResult.Insert,
                information
        );
    }
}
