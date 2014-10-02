package com.rethinkdb.ast.query.gen;

import com.google.common.collect.Lists;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.ast.helper.OptionalArguments;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.ast.query.RqlUtil;
import com.rethinkdb.model.ConflictStrategy;
import com.rethinkdb.model.Durability;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.proto.Q2L;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// extends RqlQuery
public class Table extends RqlQuery {

    public Table(RqlQuery prev, List<Object> args, java.util.Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.TABLE, args, optionalArgs);
    }

    public Insert insert(Map<String,Object> dbObject, Durability durability, Boolean returnVals, ConflictStrategy conflict) {
        return insert(Lists.newArrayList(dbObject), durability, returnVals, conflict);
    }
    public Insert insert(Map<String,Object>... dbObject) {
        return insert(Arrays.asList(dbObject), null, null, null);
    }

    public Insert insert(List<Map<String,Object>> dbObjects) {
        return insert(dbObjects, null, null, null);
    }

    public Insert insert(List<Map<String,Object>> dbObjects, Durability durability, Boolean returnVals, ConflictStrategy conflict) {
        Map<String, Object> optionalArgs = new HashMap<String, Object>();

        if (returnVals != null && returnVals == true) {
            optionalArgs.put("return_vals", true);
        }
        if (conflict != null) {
            optionalArgs.put("conflict", conflict.toString());
        }
        if (durability != null) {
            optionalArgs.put("durability", durability.toString());
        }

        return new Insert(this, new Arguments(Lists.newArrayList(dbObjects)), optionalArgs);
    }

    public Get get(Object key) {
        return new Get(this, new Arguments(key), null);
    }

    public GetAll getAll(List<Object> keys, String index) {
        return new GetAll(this, new Arguments(keys), new OptionalArguments().with("index", index));
    }
    public GetAll getAll(List<Object> keys) {
        return getAll(keys, null);
    }

    public IndexCreate indexCreate(String name) {
        return indexCreate(name, null, null);
    }

    public IndexCreate indexCreate(String name, RqlFunction function, Boolean multi) {
        Arguments args = new Arguments(name);
        if (function != null) args.add(RqlUtil.funcWrap(function));
        return new IndexCreate(this, args, new OptionalArguments().with("multi",multi));
    }

    public IndexDrop indexDrop(String name) {
        return new IndexDrop(this, new Arguments(name), null);
    }

    public IndexList indexList() {
        return new IndexList(this, null, null);
    }

    public IndexStatus indexStatus(String... indexNames) {
        return new IndexStatus(this, new Arguments(indexNames), null);
    }

    public IndexWait indexWait(String... indexNames) {
        return new IndexWait(this, new Arguments(indexNames), null);
    }

    public Info info(){
        return new Info(this, null, null);
    }

    public Changes changes() {
        return new Changes(this, null, null);
    }

    @Override
    public List<Map<String,Object>> run(RethinkDBConnection connection) {
        return (List<Map<String, Object>>) super.run(connection);
    }
}
        