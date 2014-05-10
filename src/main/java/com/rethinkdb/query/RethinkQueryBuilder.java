package com.rethinkdb.query;

import com.rethinkdb.RethinkDBConstants;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RTermBuilder;
import com.rethinkdb.query.option.Durability;
import com.rethinkdb.response.DBResult;
import com.rethinkdb.response.DMLResult;
import com.rethinkdb.response.InsertResult;
import com.rethinkdb.response.StringListDBResult;

public class RethinkQueryBuilder {

    private String dbName = RethinkDBConstants.DEFAULT_DB_NAME;
    private String tableName = RethinkDBConstants.DEFAULT_TABLE_NAME;

    public RethinkQueryBuilder db(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public RethinkQueryBuilder table(String tableName) {
        this.tableName = tableName;
        return this;
    }

    private RethinkTerminatingQuery terminatingQuery(Class<? extends DBResult> resultClazz,
                                                     QueryInformation queryInformation) {
        return new RethinkTerminatingQuery(resultClazz, queryInformation);
    }

    public RethinkTerminatingQuery dbCreate(String dbName) {
        return terminatingQuery(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_CREATE)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    public RethinkTerminatingQuery dbDrop(String dbName) {
        return terminatingQuery(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_DROP)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    public RethinkTerminatingQuery dbList() {
        return terminatingQuery(
                StringListDBResult.class,
                new QueryInformation().ofTermType(Q2L.Term.TermType.DB_LIST)
        );
    }

    public RethinkTerminatingQuery tableCreate(String tableName) {
        return tableCreate(tableName, null, null, null);
    }

    public RethinkTerminatingQuery tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.TABLE_CREATE)
                .withArgs(
                        RTermBuilder.dbTerm(dbName),
                        RTermBuilder.datumTerm(tableName)

                );

        if (primaryKey != null) {
            information.withOptArg("primary_key", RTermBuilder.datumTerm(tableName));
        }
        if (datacenter != null) {
            information.withOptArg("datacenter", RTermBuilder.datumTerm(tableName));
        }
        if (durability != null) {
            information.withOptArg("durability", RTermBuilder.datumTerm(durability.toString()));
        }

        return terminatingQuery(
                DMLResult.class,
                information
        );
    }

    public RethinkTerminatingQuery tableDrop(String tableName) {
        return terminatingQuery(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_DROP)
                        .withArgs(
                                RTermBuilder.dbTerm(dbName),
                                RTermBuilder.datumTerm(tableName)
                        )
        );
    }

    public RethinkTerminatingQuery tableList() {
        return terminatingQuery(
                StringListDBResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_LIST)
                        .withArgs(
                                RTermBuilder.dbTerm(dbName)
                        )
        );
    }

    public RethinkTerminatingQuery insert(DBObject object) {
        return insert(object, null, false, false);
    }

    public RethinkTerminatingQuery insert(DBObject object, Durability durability, boolean returnVals, boolean upsert) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.INSERT)
                .withArgs(
                        new RTermBuilder()
                                .ofType(Q2L.Term.TermType.TABLE)
                                .addArg(RTermBuilder.datumTerm(tableName))
                                .build(),

                        RTermBuilder.datumTerm(object)
                );

        if (returnVals) {
            information.withOptArg("return_vals", RTermBuilder.datumTerm(true));
        }
        if (upsert) {
            information.withOptArg("upsert", RTermBuilder.datumTerm(true));
        }
        if (durability != null) {
            information.withOptArg("durability", RTermBuilder.datumTerm(durability.toString()));
        }

        return terminatingQuery(
                InsertResult.class,
                information
        );
    }
}
