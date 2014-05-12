package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTTopLevelQuery;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.DDLResult;
import com.rethinkdb.response.model.DMLResult;

import java.util.List;

public class RTTopLevelQuery_Types {

    public static class T_DMLResult extends RTTopLevelQuery {

        public T_DMLResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public DMLResult run(RethinkDBConnection connection) {
            return DBResponseMapper.populateObject(new DMLResult(), (DBObject) super.run(connection));
        }
    }

    public static class T_DDLResult extends RTTopLevelQuery {

        public T_DDLResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public DDLResult run(RethinkDBConnection connection) {
            return DBResponseMapper.populateObject(new DDLResult(), (DBObject) super.run(connection));
        }
    }

    public static class T_StringListResult extends RTTopLevelQuery {

        public T_StringListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public List<String> run(RethinkDBConnection connection) {
            return (List<String>) super.run(connection);
        }
    }

}
