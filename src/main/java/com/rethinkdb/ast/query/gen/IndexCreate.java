package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.DDLResult;
import com.rethinkdb.response.model.DMLResult;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class IndexCreate extends RqlQuery {

    public IndexCreate(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public IndexCreate(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.INDEX_CREATE, args, optionalArgs);
    }

    @Override
    public DDLResult run(RethinkDBConnection connection) {
        return DBResponseMapper.populateObject(new DDLResult(), (Map<String, Object>) super.run(connection));
    }
}
        