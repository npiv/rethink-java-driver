package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTData;
import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.option.Durability;
import com.rethinkdb.fluent.types.RTFluentQueryTypes;
import com.rethinkdb.fluent.types.RTTopLevelQueryTypes;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
    public RTFluentQueryTypes.ObjectListResult table(String tableName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.TABLE).withArgs(tableName);
        return new RTFluentQueryTypes.ObjectListResult(treeKeeper.addData(operation));
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
    public RTTopLevelQueryTypes.DDLResult dbCreate(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_CREATE).withArgs(new RTData<String>(dbName));
        return new RTTopLevelQueryTypes.DDLResult(treeKeeper.addData(operation));
    }

    /**
     * drop database
     *
     * @param dbName database name
     */
    public RTTopLevelQueryTypes.DDLResult dbDrop(String dbName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_DROP).withArgs(new RTData<String>(dbName));
        return new RTTopLevelQueryTypes.DDLResult(treeKeeper.addData(operation));
    }

    /**
     * list databases
     */
    public RTTopLevelQueryTypes.StringListResult dbList() {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.DB_LIST);
        return new RTTopLevelQueryTypes.StringListResult(treeKeeper.addData(operation));
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public RTTopLevelQueryTypes.DMLResult insert(com.rethinkdb.model.DBObject... objects) {
        return insert(Arrays.asList(objects), null, false, false);
    }

    /**
     * insert DBObjects
     *
     * @param objects objects to insert
     */
    public RTTopLevelQueryTypes.DMLResult insert(List<com.rethinkdb.model.DBObject> objects) {
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
    public RTTopLevelQueryTypes.DMLResult insert(com.rethinkdb.model.DBObject dbObject, Durability durability, boolean returnVals, boolean upsert) {
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
    public RTTopLevelQueryTypes.DMLResult insert(List<com.rethinkdb.model.DBObject> dbObjects, Durability durability, boolean returnVals, boolean upsert) {
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

        return new RTTopLevelQueryTypes.DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Update JSON documents in a table.
     *
     * @param object a DBObject of the changes to make
     */
    public RTTopLevelQueryTypes.DMLResult update(com.rethinkdb.model.DBObject object) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.UPDATE).withArgs(new RTData<com.rethinkdb.model.DBObject>(object));
        return new RTTopLevelQueryTypes.DMLResult(treeKeeper.addData(operation));
    }

    public RTTopLevelQueryTypes.DMLResult replace(com.rethinkdb.model.DBObject dbObject) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.REPLACE).withArgs(new RTData<com.rethinkdb.model.DBObject>(dbObject));
        return new RTTopLevelQueryTypes.DMLResult(treeKeeper.addData(operation));
    }

    public RTTopLevelQueryTypes.DMLResult replace(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.REPLACE).withArgs(lambda.treeKeeper.getTree());
        return new RTTopLevelQueryTypes.DMLResult(treeKeeper.addData(operation));
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentQueryTypes.DBObject get(String key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentQueryTypes.DBObject(treeKeeper.addData(operation));
    }

    /**
     * Get a document by Primary Key
     *
     * @param key key
     */
    public RTFluentQueryTypes.DBObject get(Number key) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET).withArgs(key);
        return new RTFluentQueryTypes.DBObject(treeKeeper.addData(operation));
    }


    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQueryTypes.ObjectListResult getAll(String index, String... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQueryTypes.ObjectListResult getAll(String indexName, String index, String... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQueryTypes.ObjectListResult getAll(Number index, Number... additionalIndexes) {
        return _getAll(null, index, additionalIndexes);
    }

    /**
     * Get all documents where the given value matches the value of the requested index(es).
     *
     * @param indexName         the name of the index if not default (id)
     * @param index             at least one index mandatory
     * @param additionalIndexes optional additional indexes
     */
    public RTFluentQueryTypes.ObjectListResult getAll(String indexName, Number index, Number... additionalIndexes) {
        return _getAll(indexName, index, additionalIndexes);
    }

    private <INDEX> RTFluentQueryTypes.ObjectListResult _getAll(String indexName, INDEX index, INDEX... additionalIndexes) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_ALL);

        operation.withArgs(index);
        for (INDEX k : additionalIndexes) {
            operation.withArgs(k);
        }

        return new RTFluentQueryTypes.ObjectListResult(treeKeeper.addData(operation));
    }

    public RTFluentQueryTypes.DoubleListResult mapToDouble(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentQueryTypes.DoubleListResult(treeKeeper.addData(operation));
    }

    public RTFluentQueryTypes.StringListResult mapToString(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentQueryTypes.StringListResult(treeKeeper.addData(operation));
    }

    public RTFluentQueryTypes.GenericListResult map(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.MAP).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentQueryTypes.GenericListResult(treeKeeper.addData(operation));
    }

    public RTFluentQueryTypes.ObjectListResult filter(RTFluentQuery lambda) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.FILTER).withArgs(lambda.treeKeeper.getTree());
        return new RTFluentQueryTypes.ObjectListResult(treeKeeper.addData(operation));
    }

    public RTFluentQueryTypes.ObjectListResult without(String field) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.WITHOUT).withArgs(field);
        return new RTFluentQueryTypes.ObjectListResult(treeKeeper.addData(operation));
    }

    public RTFluentQuery lambda(DBLambda lambda) {
        List<Object> params = new ArrayList<Object>();
        params.add(1);
        RTOperation operation = new RTOperation(Q2L.Term.TermType.FUNC).withArgs(params);

        RTFluentQuery varQuery = new RTFluentQuery(new RTTreeKeeper().addData(new RTOperation(Q2L.Term.TermType.VAR).withArgs(new RTData(1))));

        RTOperation applied = lambda.apply(varQuery).treeKeeper.getTree();

        return new RTFluentQuery(treeKeeper.addData(operation.withArgs(applied)));
    }

    public RTFluentQuery field(String fieldName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_FIELD).withArgs(fieldName);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    private RTFluentQuery makeOperation(Q2L.Term.TermType type, Object... arg) {
        RTOperation operation = new RTOperation(type).withArgs(arg);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    public RTFluentQuery upcase() {
        return new RTFluentQuery(treeKeeper.addData(new RTOperation(Q2L.Term.TermType.UPCASE)));
    }

    public RTFluentQuery downcase() {
        return new RTFluentQuery(treeKeeper.addData(new RTOperation(Q2L.Term.TermType.DOWNCASE)));
    }

    public RTFluentQuery split() {
        return new RTFluentQuery(treeKeeper.addData(new RTOperation(Q2L.Term.TermType.SPLIT)));
    }

    public RTFluentQuery split(String separator) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.SPLIT).withArgs(separator);
        return new RTFluentQuery(treeKeeper.addData(operation));
    }

    public RTFluentQuery split(String separator, int maxTimes) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.SPLIT).withArgs(separator, maxTimes);
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
