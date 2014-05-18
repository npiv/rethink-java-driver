package com.rethinkdb.ast.query.gen;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.proto.Q2L;

import java.util.List;
import java.util.Map;

// extends RqlMethodQuery
public class Group extends RqlQuery {

    public Group(List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, args, optionalArgs);
    }

    public Group(RqlQuery prev, List<Object> args, Map<String, Object> optionalArgs) {
        super(prev, Q2L.Term.TermType.GROUP, args, optionalArgs);
    }

//    @Override
//    public Map<String, List<Map<String,Object>>> run(RethinkDBConnection connection) {
//        return (Map<String, List<Map<String, Object>>>) ((Map<String,Object>)super.run(connection)).get("data");
//    }
}
        