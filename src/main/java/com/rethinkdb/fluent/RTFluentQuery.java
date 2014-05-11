package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTData;
import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.query.option.Durability;
import com.rethinkdb.response.DMLResult;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RTFluentQuery<T> extends RTTopLevelQuery<T> {

    private static final AtomicInteger tokenGenerator = new AtomicInteger();

    public RTFluentQuery() {
    }

    public RTFluentQuery(RTTreeKeeper treeKeeper, Class<T> sampleClass) {
        super(treeKeeper, sampleClass);
    }

    // TODO: this is duplicated across DBQuery and here

    /**
     * Select the table to operate on
     *
     * @param tableName table name
     */
    public RTFluentQuery<DBObject> table(String tableName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE).withArgs(tableName);

        return new RTFluentQuery(treeKeeper.addData(operation), DBObject.class);
    }

    /**
     * Select the database to operate on
     *
     * @param dbName database name
     */
    public RTDBQuery db(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB).withArgs(new RTData<String>(dbName));
        return new RTDBQuery(treeKeeper.addData(operation), DBObject.class);
    }

    /**
     * create database
     *
     * @param dbName database name
     */
    public RTTopLevelQuery<DMLResult> dbCreate(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_CREATE).withArgs(new RTData<String>(dbName));
        return new RTTopLevelQuery(treeKeeper.addData(operation), DMLResult.class);
    }

    /**
     * drop database
     *
     * @param dbName database name
     */
    public RTTopLevelQuery<DMLResult> dbDrop(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_DROP).withArgs(new RTData<String>(dbName));
        return new RTTopLevelQuery(treeKeeper.addData(operation), DMLResult.class);
    }

    /**
     * list databases
     */
    public RTTopLevelQuery_StringList dbList() {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_LIST);
        return new RTTopLevelQuery_StringList(treeKeeper.addData(operation));
    }

    public RTFluentQuery<DMLResult> update(DBObject object) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.UPDATE)
                .withArgs(new RTData<DBObject>(object));

        return new RTFluentQuery(treeKeeper.addData(operation), DMLResult.class);
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public RTTopLevelQuery<DMLResult> insert(DBObject... objects) {
        return insert(Arrays.asList(objects), null, false, false);
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public RTTopLevelQuery<DMLResult> insert(List<DBObject> objects) {
        return insert(objects, null, false, false);
    }

    /**
     * Insert one DBObject into the database
     *
     * @param dbObject   dbObject
     * @param durability Hard or Soft (leave null for default)
     * @param returnVals if set to true and in case of a single insert/upsert, the inserted/updated document will be returned.
     * @param upsert     when set to true, performs a replace if a document with the same primary key exists.
     */
    public RTTopLevelQuery<DMLResult> insert(DBObject dbObject, Durability durability, boolean returnVals, boolean upsert) {
        return insert(Arrays.asList(dbObject), durability, returnVals, upsert);
    }

    /**
     * Insert a list of DBObjects into the database
     *
     * @param dbObjects  dbObjects
     * @param durability Hard or Soft (leave null for default)
     * @param returnVals if set to true and in case of a single insert/upsert, the inserted/updated document will be returned.
     * @param upsert     when set to true, performs a replace if a document with the same primary key exists.
     */
    public RTTopLevelQuery<DMLResult> insert(List<DBObject> dbObjects, Durability durability, boolean returnVals, boolean upsert) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.INSERT);

        if (returnVals) {
            operation.withOptionalArg("return_vals", true);
        }
        if (upsert) {
            operation.withOptionalArg("upsert", true);
        }
        if (durability != null) {
            operation.withOptionalArg("durability", durability.toString());
        }

        if (dbObjects.size() == 1) {
            operation.withArgs(dbObjects.get(0));
        } else {
            operation.withArgs(dbObjects);
        }

        return new RTTopLevelQuery<DMLResult>(treeKeeper.addData(operation), DMLResult.class);
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentQuery<DBObject> get(String key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentQuery<DBObject>(treeKeeper.addData(operation), DBObject.class);
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentQuery<DBObject> get(Number key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentQuery<DBObject>(treeKeeper.addData(operation), DBObject.class);
    }


    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQuery<List> getAll(String index, String... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQuery<List> getAll(String indexName, String index, String... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQuery<List> getAll(Number index, Number... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQuery<List> getAll(String indexName, Number index, Number... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    private <INDEX> RTFluentQuery<List> _getAll(String indexName, INDEX index, INDEX... additionalIndexes) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_ALL);

        operation.withArgs(index);
        for (INDEX k : additionalIndexes) {
            operation.withArgs(k);
        }

        return new RTFluentQuery<List>(treeKeeper.addData(operation), List.class);
    }

}
