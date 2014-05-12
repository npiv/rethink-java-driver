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

import java.util.ArrayList;
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

    /**
     * Replace documents in a selection
     *
     * @param dbObject the changes to make
     */
    public RTTopLevelQuery_Types.T_DMLResult replace(DBObject dbObject) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.REPLACE).withArgs(new RTData<DBObject>(dbObject));
        return new RTTopLevelQuery_Types.T_DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Replace documents in a selection
     *
     * @param lambda the changes to make
     */
    public T_DMLResult replace(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.REPLACE).withArgs(lambda.treeKeeper.getTree());
        return new T_DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Delete documents in a selection
     */
    public T_DMLResult delete() {
        return delete(null, null);
    }

    /**
     * Delete documents in a selection
     * @param durability Hard or Soft (leave null for default)
     * @param returnVals if set to true the deleted document will be returned.
     */
    public T_DMLResult delete(Durability durability, Boolean returnVals) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DELETE);
        if (returnVals) {
            operation.withOptionalArg("return_vals", true);
        }
        if (durability != null) {
            operation.withOptionalArg("durability", durability.toString());
        }
        return new T_DMLResult(treeKeeper.addData(operation));
    }

    /**
     * sync ensures that writes on a given table are written to permanent storage.
     * Queries that specify soft durability (durability='soft') do not give such guarantees,
     * so sync can be used to ensure the state of these queries. A call to sync does not return until
     * all previous writes to the table are persisted.
     */
    public RTFluentQuery sync() {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.SYNC);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    /**
     * Plucks out one or more attributes
     * @param fields fields to pluck
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult pluck(List<String> fields) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.PLUCK).withArgs(fields);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    /**
     * Plucks out one or more attributes from nested objects
     * @param dbObjects dbObjects describing fields to pluck.
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult pluckNested(List<DBObject> dbObjects) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.PLUCK).withArgs(dbObjects);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
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

    /**
     * Transform each element of the sequence by applying the given mapping function. Returns a List of Doubles
     * @param lambda the transformation to apply
     */
    public RTFluentLevelQuery_Types.T_DoubleListResult mapToDouble(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentLevelQuery_Types.T_DoubleListResult(treeKeeper.addData(operation));
    }

    /**
     * Transform each element of the sequence by applying the given mapping function. Returns a List of Strings
     * @param lambda the transformation to apply
     */
    public RTFluentLevelQuery_Types.T_StringListResult mapToString(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentLevelQuery_Types.T_StringListResult(treeKeeper.addData(operation));
    }

    /**
     * Transform each element of the sequence by applying the given mapping function. Returns a generic List
     * @param lambda the transformation to apply
     */
    public RTFluentLevelQuery_Types.T_GenericListResult map(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentLevelQuery_Types.T_GenericListResult(treeKeeper.addData(operation));
    }

    /**
     * Get all the documents for which the given predicate is true.
     * @param lambda predicate
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult filter(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.FILTER).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    /**
     * Get all the documents which match the given example
     * @param example example
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult filter(DBObject example) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.FILTER).withArgs(example);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    /**
     * The opposite of pluck. Takes an object or a sequence of objects, and returns them with the specified paths removed.
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult without(String field, String... additionalFields) {
        List<String> fields = Arrays.asList(additionalFields);
        fields.add(0, field);
        return without(fields);
    }

    /**
     * The opposite of pluck. Takes an object or a sequence of objects, and returns them with the specified paths removed.
     */
    public RTFluentLevelQuery_Types.T_ObjectListResult without(List<String> fields) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.WITHOUT).withArgs(fields);
        return new RTFluentLevelQuery_Types.T_ObjectListResult(treeKeeper.addData(operation));
    }

    /**
     * Opens a lambda.
     *
     * In java 1.6, 1.7 Should be invoked as r.lambda(new DBLambda() { ... }) <br />
     * From java 1.8 can be invoked as r.lambda(x-> ...);
     */
    public RTFluentQuery lambda(DBLambda lambda) {
        RTOperation functionOperation = new RTOperation(Q2L.Term.TermType.FUNC).withArgs(Arrays.asList(1));

        RTFluentQuery varQuery = new RTFluentQuery(new RTTreeKeeper().addData(new RTOperation(Q2L.Term.TermType.VAR).withArgs(new RTData(1))));

        RTOperation applied = lambda.apply(varQuery).treeKeeper.getTree();

        return new RTFluentQuery(treeKeeper.addData(functionOperation.withArgs(applied)));
    }

    /**
     * Get the field name
     * @param fieldName field name
     */
    public RTFluentQuery field(String fieldName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_FIELD).withArgs(fieldName);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    private RTFluentQuery makeOperation(Q2L.Term.TermType type, Object... arg) {
        RTOperation operation = new RTOperation(type).withArgs(arg);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    public RTFluentQuery add(double op) {
        return makeOperation(Q2L.Term.TermType.ADD, op);
    }

    public RTFluentQuery add(String op) {
        return makeOperation(Q2L.Term.TermType.ADD, op);
    }

    public RTFluentQuery subtract(double op) {
        return makeOperation(Q2L.Term.TermType.SUB, op);
    }

    public RTFluentQuery multiply(double op) {
        return makeOperation(Q2L.Term.TermType.MUL, op);
    }

    public RTFluentQuery divide(double op) {
        return makeOperation(Q2L.Term.TermType.DIV, op);
    }

    public RTFluentQuery modulus(double op) {
        return makeOperation(Q2L.Term.TermType.MOD, op);
    }

    public RTFluentQuery and(double op) {
        return makeOperation(Q2L.Term.TermType.MOD, op);
    }

    public RTFluentQuery lt(Object... o) {
        return makeOperation(Q2L.Term.TermType.LT, o);
    }

    public RTFluentQuery gt(Object... o) {
        return makeOperation(Q2L.Term.TermType.GT, o);
    }

    public RTFluentQuery le(Object... o) {
        return makeOperation(Q2L.Term.TermType.LE, o);
    }

    public RTFluentQuery ge(Object... o) {
        return makeOperation(Q2L.Term.TermType.GE, o);
    }

    public RTFluentQuery eq(Object... o) {
        return makeOperation(Q2L.Term.TermType.EQ, o);
    }

    public RTFluentQuery ne(Object... o) {
        return makeOperation(Q2L.Term.TermType.NE, o);
    }

    public RTFluentQuery not(Object... o) {
        return makeOperation(Q2L.Term.TermType.NOT, o);
    }

    public RTFluentQuery all(Object... o) {
        return makeOperation(Q2L.Term.TermType.ALL, o);
    }

    public RTFluentQuery and(Object... o) {
        return makeOperation(Q2L.Term.TermType.ALL, o);
    }

    public RTFluentQuery any(Object... o) {
        return makeOperation(Q2L.Term.TermType.ANY, o);
    }

    public RTFluentQuery or(Object... o) {
        return makeOperation(Q2L.Term.TermType.ANY, o);
    }
}
