package com.rethinkdb.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBConstants;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RTermBuilder;
import com.rethinkdb.query.option.Durability;
import com.rethinkdb.response.DMLResult;
import com.rethinkdb.response.InsertResult;

import java.util.Arrays;
import java.util.List;

/**
 * This class is the entry point for all the query, insert, dml actions that we can
 * perform on a RethinkDB Instance.
 */
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

    public DBObject run(RethinkDBConnection connection) {
        return new RethinkTerminatingQuery<DBObject>(
                DBObject.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE)
                        .withArg(RTermBuilder.dbTerm(dbName))
                        .withArgs(RTermBuilder.datumTerm(tableName))
        ).run(connection);
    }
    // todo: should be able to run() after table() but optional

    public RethinkTerminatingQuery<DMLResult> dbCreate(String dbName) {
        return new RethinkTerminatingQuery<DMLResult>(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_CREATE)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    public RethinkTerminatingQuery<DMLResult> dbDrop(String dbName) {
        return new RethinkTerminatingQuery<DMLResult>(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_DROP)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    public RethinkTerminatingQuery<List> dbList() {
        return new RethinkTerminatingQuery<List>(
                List.class,
                new QueryInformation().ofTermType(Q2L.Term.TermType.DB_LIST)
        );
    }

    public RethinkTerminatingQuery<DMLResult> tableCreate(String tableName) {
        return tableCreate(tableName, null, null, null);
    }

    /**
     * Create table with tableName, primaryKey, Durability on a specific dataCenter.
     *
     * @param tableName  tableName (mandatory)
     * @param primaryKey primary key (leave null for default)
     * @param durability durability  (leave null for default)
     * @param datacenter datacenter  (leave null for default)
     * @return query
     */
    public RethinkTerminatingQuery<DMLResult> tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.TABLE_CREATE)
                .withArgs(
                        RTermBuilder.dbTerm(dbName),
                        RTermBuilder.datumTerm(tableName)

                );

        if (datacenter != null) {
            information.withOptArg("datacenter", RTermBuilder.datumTerm(datacenter));
        }
        if (primaryKey != null) {
            information.withOptArg("primary_key", RTermBuilder.datumTerm(primaryKey));
        }
        if (durability != null) {
            information.withOptArg("durability", RTermBuilder.datumTerm(durability.toString()));
        }

        return new RethinkTerminatingQuery<DMLResult>(
                DMLResult.class,
                information
        );
    }

    public RethinkTerminatingQuery<DMLResult> tableDrop(String tableName) {
        return new RethinkTerminatingQuery<DMLResult>(
                DMLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_DROP)
                        .withArgs(
                                RTermBuilder.dbTerm(dbName),
                                RTermBuilder.datumTerm(tableName)
                        )
        );
    }

    public RethinkTerminatingQuery<List> tableList() {
        return new RethinkTerminatingQuery<List>(
                List.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_LIST)
                        .withArgs(
                                RTermBuilder.dbTerm(dbName)
                        )
        );
    }

    public RethinkTerminatingQuery<InsertResult> insert(DBObject... objects) {
        return insert(Arrays.asList(objects), null, false, false);
    }

    public RethinkTerminatingQuery<InsertResult> insert(List<DBObject> objects) {
        return insert(objects, null, false, false);
    }

    public RethinkTerminatingQuery<InsertResult> insert(DBObject dbObject, Durability durability, boolean returnVals, boolean upsert) {
        return insert(Arrays.asList(dbObject), durability, returnVals, upsert);
    }

    public RethinkTerminatingQuery<InsertResult> insert(List<DBObject> dbObjects, Durability durability, boolean returnVals, boolean upsert) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.INSERT)
                .withArgs(RTermBuilder.tableTerm(tableName));

        if (dbObjects.size() == 1) {
            information.withArg(RTermBuilder.datumTerm(dbObjects.get(0)));
        } else {
            Q2L.Term.Builder builder = Q2L.Term.newBuilder().setType(Q2L.Term.TermType.MAKE_ARRAY);
            for (DBObject dbObject : dbObjects) {
                builder.addArgs(RTermBuilder.datumTerm(dbObject));
            }
            information.withArg(builder.build());
        }

        if (returnVals) {
            information.withOptArg("return_vals", RTermBuilder.datumTerm(true));
        }
        if (upsert) {
            information.withOptArg("upsert", RTermBuilder.datumTerm(true));
        }
        if (durability != null) {
            information.withOptArg("durability", RTermBuilder.datumTerm(durability.toString()));
        }

        return new RethinkTerminatingQuery<InsertResult>(
                InsertResult.class,
                information
        );
    }

    public RethinkTerminatingQuery<InsertResult> getAll(String key, String... keys) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.GET_ALL);

        information.withArg(RTermBuilder.tableTerm(tableName));
        information.withArg(RTermBuilder.datumTerm(key));
        for (String k : keys) {
            information.withArg(RTermBuilder.datumTerm(k));
        }

        return new RethinkTerminatingQuery<InsertResult>(
                InsertResult.class,
                information
        );
    }
}
