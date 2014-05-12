package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTTopLevelQuery;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.DDLResult;
import com.rethinkdb.response.model.DMLResult;

import java.util.List;

public class RTTopLevelQueryTypes {

    public static class DMLResult extends RTTopLevelQuery {

        public DMLResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public com.rethinkdb.response.model.DMLResult run(RethinkDBConnection connection) {
            return DBResponseMapper.populateObject(new com.rethinkdb.response.model.DMLResult(), (DBObject) super.run(connection));
        }
    }

    public static class DDLResult extends RTTopLevelQuery {

        public DDLResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public com.rethinkdb.response.model.DDLResult run(RethinkDBConnection connection) {
            return DBResponseMapper.populateObject(new com.rethinkdb.response.model.DDLResult(), (DBObject) super.run(connection));
        }
    }

    public static class StringListResult extends RTTopLevelQuery {

        public StringListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public List<String> run(RethinkDBConnection connection) {
            return (List<String>) super.run(connection);
        }
    }

}
