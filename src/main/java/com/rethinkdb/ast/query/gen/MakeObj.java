package com.rethinkdb.ast.query.gen;

import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;

// extends RqlQuery
public class MakeObj extends RqlQuery {

    public MakeObj(java.util.Map<String, Object> fields) {
        super(Q2L.Term.TermType.MAKE_OBJ, new ArrayList<Object>(), fields);
    }
}