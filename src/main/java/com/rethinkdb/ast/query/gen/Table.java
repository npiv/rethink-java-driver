package com.rethinkdb.ast.query.gen;

import com.google.common.collect.Lists;
import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.ast.helper.OptionalArguments;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.Durability;
import com.rethinkdb.proto.Q2L;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// extends RqlQuery
public class Table extends RqlQuery {

    public Table(RqlQuery prev, List<Object> args, java.util.Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.TABLE, args, optionalArgs);
    }

    public Insert insert(Map<String,Object> dbObject, Durability durability, Boolean returnVals, Boolean upsert) {
        return insert(Lists.newArrayList(dbObject), durability, returnVals, upsert);
    }
    public Insert insert(Map<String,Object> dbObject) {
        return insert(dbObject, null, null, null);
    }

    public Insert insert(List<Map<String,Object>> dbObjects) {
        return insert(dbObjects, null, null, null);
    }

    public Insert insert(List<Map<String,Object>> dbObjects, Durability durability, Boolean returnVals, Boolean upsert) {
        Map<String, Object> optionalArgs = new HashMap<String, Object>();

        if (returnVals != null && returnVals == true) {
            optionalArgs.put("return_vals", true);
        }
        if (upsert != null) {
            optionalArgs.put("upsert", true);
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
}
        