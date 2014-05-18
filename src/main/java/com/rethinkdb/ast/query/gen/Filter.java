package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class Filter extends RqlQuery {

    public Filter(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public Filter(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.FILTER, args, optionalArgs);
    }

    @Override
    public List<Map<String,Object>> run(RethinkDBConnection connection) {
        return (List<Map<String,Object>>)super.run(connection);
    }
}
        