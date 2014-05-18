package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.IndexStatusResult;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class IndexStatus extends RqlQuery {

    public IndexStatus(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public IndexStatus(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.INDEX_STATUS, args, optionalArgs);
    }

    @Override
    public List<IndexStatusResult> run(RethinkDBConnection connection) {
        return DBResponseMapper.populateList(IndexStatusResult.class, (List<Map<String, Object>>) super.run(connection));
    }
}
        