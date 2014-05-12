package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.fluent.RTTopLevelQuery;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.response.DBResponseMapper;
import com.rethinkdb.response.model.DDLResult;
import com.rethinkdb.response.model.DMLResult;

import java.util.List;

public class RTFluentLevelQuery_Types {

    public static class T_GenericListResult extends RTFluentQuery {

        public T_GenericListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @SuppressWarnings("unchecked")
        public List run(RethinkDBConnection connection) {
            return (List) super.run(connection);
        }
    }

    public static class T_ObjectListResult extends RTFluentQuery {

        public T_ObjectListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<DBObject> run(RethinkDBConnection connection) {
            return (List<DBObject>) super.run(connection);
        }
    }

    public static class T_DoubleListResult extends RTFluentQuery {

        public T_DoubleListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Double> run(RethinkDBConnection connection) {
            return (List<Double>) super.run(connection);
        }
    }

    public static class T_StringListResult extends RTFluentQuery {

        public T_StringListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<String> run(RethinkDBConnection connection) {
            return (List<String>) super.run(connection);
        }
    }

    public static class T_DBObject extends RTFluentQuery {

        public T_DBObject(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public DBObject run(RethinkDBConnection connection) { return (DBObject) super.run(connection); }
    }

}
