package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class IndexList extends RqlQuery {

    public IndexList(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public IndexList(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.INDEX_LIST, args, optionalArgs);
    }

    @Override
    public List<String> run(RethinkDBConnection connection) {
        return (List<String>) super.run(connection);
    }
}
        