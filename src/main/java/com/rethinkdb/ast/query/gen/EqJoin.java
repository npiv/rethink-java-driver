package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.JoinResult;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class EqJoin extends RqlQuery {

    public EqJoin(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public EqJoin(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.EQ_JOIN, args, optionalArgs);
    }

    @Override
    public List<JoinResult> run(RethinkDBConnection connection) {
        return DBResponseMapper.populateList(JoinResult.class, (List<Map<String, Object>>) super.run(connection));
    }
}
        