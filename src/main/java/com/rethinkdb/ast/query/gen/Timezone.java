package com.rethinkdb.ast.query.gen;

import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class Timezone extends RqlQuery {

    public Timezone(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public Timezone(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.TIMEZONE, args, optionalArgs);
    }
}
        