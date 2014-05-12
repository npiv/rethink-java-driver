package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTData;
import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.option.Durability;
import com.rethinkdb.fluent.types.RTFluentLevelQuery_Types;
import com.rethinkdb.fluent.types.RTTopLevelQuery_Types;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.model.DMLResult;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.rethinkdb.fluent.types.RTTopLevelQuery_Types.*;

public class RTFluentQuery<T> extends RTTopLevelQuery<T> {

    private static final AtomicInteger tokenGenerator = new AtomicInteger();

    public RTFluentQuery() {
    }

    public RTFluentQuery(RTTreeKeeper treeKeeper) {
        super(treeKeeper);
    }

    /**
     * Select the table to operate on
     *
     * @param tableName table name
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult table(String tableName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE).withArgs(tableName);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    /**
     * Select the database to operate on
     *
     * @param dbName database name
     */
    public RTDBQuery db(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB).withArgs(new RTData<String>(dbName));
        return new RTDBQuery(treeKeeper.addData(operation));
    }

    /**
     * create database
     *
     * @param dbName database name
     */
    public T_DDLResult dbCreate(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_CREATE).withArgs(new RTData<String>(dbName));
        return new T_DDLResult(treeKeeper.addData(operation));
    }

    /**
     * drop database
     *
     * @param dbName database name
     */
    public T_DDLResult dbDrop(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_DROP).withArgs(new RTData<String>(dbName));
        return new T_DDLResult(treeKeeper.addData(operation));
    }

    /**
     * list databases
     */
    public T_StringListResult dbList() {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_LIST);
        return new T_StringListResult(treeKeeper.addData(operation));
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public T_DMLResult insert(DBObject... objects) {
        return insert(Arrays.asList(objects), null, false, false);
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public T_DMLResult insert(List<DBObject> objects) {
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
    public T_DMLResult insert(DBObject dbObject, Durability durability, boolean returnVals, boolean upsert) {
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
    public T_DMLResult insert(List<DBObject> dbObjects, Durability durability, boolean returnVals, boolean upsert) {
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

        return new T_DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Update JSON documents in a table.
     *
     * @param object a DBObject of the changes to make
     */
    public RTTopLevelQuery_Types.T_DMLResult update(DBObject object) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.UPDATE).withArgs(new RTData<DBObject>(object));
        return new RTTopLevelQuery_Types.T_DMLResult(treeKeeper.addData(operation));
    }

    public RTTopLevelQuery_Types.T_DMLResult replace(DBObject dbObject) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.REPLACE).withArgs(new RTData<DBObject>(dbObject));
        return new RTTopLevelQuery_Types.T_DMLResult(treeKeeper.addData(operation));
    }

    public T_DMLResult replace(DBLambda lambda) {
        RTOperation operation = RTOperation.lambda(Q2L.Term.TermType.REPLACE, lambda);
        return new T_DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentLevelQuery_Types.T_DBObject get(String key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentLevelQuery_Types.T_DBObject(treeKeeper.addData(operation));
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentLevelQuery_Types.T_DBObject get(Number key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentLevelQuery_Types.T_DBObject(treeKeeper.addData(operation));
    }


    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult getAll(String index, String... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult getAll(String indexName, String index, String... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult getAll(Number index, Number... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult getAll(String indexName, Number index, Number... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    private <INDEX> RTFluentLevelQuery_Types.T_ObjectListResult _getAll(String indexName, INDEX index, INDEX... additionalIndexes) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_ALL);

        operation.withArgs(index);
        for (INDEX k : additionalIndexes) {
            operation.withArgs(k);
        }

        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    public RTFluentLevelQuery_Types.T_DoubleListResult mapToDouble(DBLambda lambda) {
        RTOperation operation = RTOperation.lambda(Q2L.Term.TermType.MAP, lambda);
        return new RTFluentLevelQuery_Types.T_DoubleListResult(treeKeeper.addData(operation));
    }

    public RTFluentLevelQuery_Types.T_GenericListResult map(DBLambda lambda) {
        RTOperation operation = RTOperation.lambda(Q2L.Term.TermType.MAP, lambda);
        return new RTFluentLevelQuery_Types.T_GenericListResult(treeKeeper.addData(operation));
    }


    public RTFluentLevelQuery_Types.T_ObjectListResult filter(DBLambda lambda) {
        RTOperation operation = RTOperation.lambda(Q2L.Term.TermType.FILTER, lambda);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    public RTFluentLevelQuery_Types.T_ObjectListResult without(String field) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.WITHOUT).withArgs(field);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

}
