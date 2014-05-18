package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.JoinResult;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class InnerJoin extends RqlQuery {

    public InnerJoin(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public InnerJoin(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.INNER_JOIN, args, optionalArgs);
    }

    @Override
    public List<JoinResult> run(RethinkDBConnection connection) {
        return DBResponseMapper.populateList(JoinResult.class, (List<Map<String, Object>>) super.run(connection));
    }
}
        