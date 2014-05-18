package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class SpliceAt extends RqlQuery {

    public SpliceAt(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public SpliceAt(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.SPLICE_AT, args, optionalArgs);
    }

    @Override
    public  List  run(RethinkDBConnection connection) {
        return (List) super.run(connection);
    }
}
        