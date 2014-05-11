package com.rethinkdb.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBConstants;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RTermBuilder;
import com.rethinkdb.query.option.Durability;
import com.rethinkdb.response.DDLResult;
import com.rethinkdb.response.DMLResult;

import java.util.Arrays;
import java.util.List;

/**
 * This class is the entry point for all the query, insert, dml actions that we can
 * perform on a RethinkDB Instance.
 */
public class RethinkQueryBuilder {

    private String dbName = RethinkDBConstants.DEFAULT_DB_NAME;
    private String tableName = RethinkDBConstants.DEFAULT_TABLE_NAME;

    /**
     * Select the database to use
     * @param dbName database name
     */
    public RethinkQueryBuilder db(String dbName) {
        this.dbName = dbName;
        return this;
    }

    /**
     * Select the table to operate on
     * @param tableName table name
     */
    public RethinkQueryBuilder table(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Get all results for the selected database and table (or use default test/test)
     */
    public List run(RethinkDBConnection connection) {
        return new RethinkTerminatingQuery<List>(
                List.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE)
                        .withArg(RTermBuilder.dbTerm(dbName))
                        .withArgs(RTermBuilder.datumTerm(tableName))
        ).run(connection);
    }

    /**
     * create database
     * @param dbName database name
     */
    public RethinkTerminatingQuery<DDLResult> dbCreate(String dbName) {
        return new RethinkTerminatingQuery<DDLResult>(
                DDLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_CREATE)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    /**
     * drop database
     * @param dbName database name
     */
    public RethinkTerminatingQuery<DDLResult> dbDrop(String dbName) {
        return new RethinkTerminatingQuery<DDLResult>(
                DDLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.DB_DROP)
                        .withArgs(RTermBuilder.datumTerm(dbName))
        );
    }

    /**
     * list databases
     */
    public RethinkTerminatingQuery<List> dbList() {
        return new RethinkTerminatingQuery<List>(
                List.class,
                new QueryInformation().ofTermType(Q2L.Term.TermType.DB_LIST)
        );
    }

    /**
     * create table
     * @param tableName table name
     */
    public RethinkTerminatingQuery<DDLResult> tableCreate(String tableName) {
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
    public RethinkTerminatingQuery<DDLResult> tableCreate(String tableName, String primaryKey, Durability durability, String datacenter) {
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

        return new RethinkTerminatingQuery<DDLResult>(
                DDLResult.class,
                information
        );
    }

    /**
     * drop table
     * @param tableName table name
     */
    public RethinkTerminatingQuery<DDLResult> tableDrop(String tableName) {
        return new RethinkTerminatingQuery<DDLResult>(
                DDLResult.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.TABLE_DROP)
                        .withArgs(
                                RTermBuilder.dbTerm(dbName),
                                RTermBuilder.datumTerm(tableName)
                        )
        );
    }

    /**
     * list tables
     */
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

    /**
     * insert DBObjects
     * @param objects objects to insert
     */
    public RethinkTerminatingQuery<DMLResult> insert(DBObject... objects) {
        return insert(Arrays.asList(objects), null, false, false);
    }

    /**
     * insert DBObjects
     * @param objects objects to insert
     */
    public RethinkTerminatingQuery<DMLResult> insert(List<DBObject> objects) {
        return insert(objects, null, false, false);
    }

    /**
     * Insert one DBObject into the database
     *
     * @param dbObject dbObject
     * @param durability Hard or Soft (leave null for default)
     * @param returnVals if set to true and in case of a single insert/upsert, the inserted/updated document will be returned.
     * @param upsert  when set to true, performs a replace if a document with the same primary key exists.
     */
    public RethinkTerminatingQuery<DMLResult> insert(DBObject dbObject, Durability durability, boolean returnVals, boolean upsert) {
        return insert(Arrays.asList(dbObject), durability, returnVals, upsert);
    }

    /**
     * Insert a list of DBObjects into the database
     *
     * @param dbObjects dbObjects
     * @param durability Hard or Soft (leave null for default)
     * @param returnVals if set to true and in case of a single insert/upsert, the inserted/updated document will be returned.
     * @param upsert  when set to true, performs a replace if a document with the same primary key exists.
     */
    public RethinkTerminatingQuery<DMLResult> insert(List<DBObject> dbObjects, Durability durability, boolean returnVals, boolean upsert) {
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

        return new RethinkTerminatingQuery<DMLResult>(
                DMLResult.class,
                information
        );
    }

    /**
     * Get a document by Primary Key
     * @param key key
     */
    public RethinkTerminatingQuery<DBObject> get(String key) {
        return new RethinkTerminatingQuery<DBObject>(
                DBObject.class,
                new QueryInformation()
                    .ofTermType(Q2L.Term.TermType.GET)
                    .withArgs(
                            RTermBuilder.tableTerm(tableName),
                            RTermBuilder.datumTerm(key)
        ));
    }

    /**
     * Get a document by Primary Key
     * @param key key
     */
    public RethinkTerminatingQuery<DBObject> get(Number key) {
        return new RethinkTerminatingQuery<DBObject>(
                DBObject.class,
                new QueryInformation()
                        .ofTermType(Q2L.Term.TermType.GET)
                        .withArgs(
                                RTermBuilder.tableTerm(tableName),
                                RTermBuilder.datumTerm(key)
                        ));
    }


    /**
     * Get all documents where the given value matches the value of the requested index(es).
     * @param index at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RethinkTerminatingQuery<List> getAll(String index, String... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     * @param indexName the name of the index if not default (id)
     * @param index at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RethinkTerminatingQuery<List> getAll(String indexName, String index, String... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     * @param index at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RethinkTerminatingQuery<List> getAll(Number index, Number... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     * @param indexName the name of the index if not default (id)
     * @param index at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RethinkTerminatingQuery<List> getAll(String indexName, Number index, Number... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    private <INDEX> RethinkTerminatingQuery<List> _getAll(String indexName, INDEX index, INDEX... additionalIndexes) {
        QueryInformation information = new QueryInformation()
                .ofTermType(Q2L.Term.TermType.GET_ALL);

        information.withArg(RTermBuilder.tableTerm(tableName));
        information.withArg(RTermBuilder.createDatumTerm(index));
        for (INDEX k : additionalIndexes) {
            information.withArg(RTermBuilder.createDatumTerm(k));
        }

        return new RethinkTerminatingQuery<List>(
                List.class,
                information
        );
    }
}
