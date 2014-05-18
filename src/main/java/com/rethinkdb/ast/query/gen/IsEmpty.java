package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class IsEmpty extends RqlQuery {

    public IsEmpty(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public IsEmpty(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.IS_EMPTY, args, optionalArgs);
    }

    @Override
    public Boolean run(RethinkDBConnection connection) {
        return (Boolean) super.run(connection);
    }
}
        