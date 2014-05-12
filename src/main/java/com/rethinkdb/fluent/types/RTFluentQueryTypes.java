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

public class RTFluentQueryTypes {

    public static class GenericListResult extends RTFluentQuery {

        public GenericListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @SuppressWarnings("unchecked")
        public List run(RethinkDBConnection connection) {
            return (List) super.run(connection);
        }
    }

    public static class ObjectListResult extends RTFluentQuery {

        public ObjectListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<com.rethinkdb.model.DBObject> run(RethinkDBConnection connection) {
            return (List<com.rethinkdb.model.DBObject>) super.run(connection);
        }
    }

    public static class DoubleListResult extends RTFluentQuery {

        public DoubleListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Double> run(RethinkDBConnection connection) {
            return (List<Double>) super.run(connection);
        }
    }

    public static class StringListResult extends RTFluentQuery {

        public StringListResult(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<String> run(RethinkDBConnection connection) {
            return (List<String>) super.run(connection);
        }
    }

    public static class DBObject extends RTFluentQuery {

        public DBObject(RTTreeKeeper treeKeeper) {
            super(treeKeeper);
        }

        @Override
        public com.rethinkdb.model.DBObject run(RethinkDBConnection connection) { return (com.rethinkdb.model.DBObject) super.run(connection); }
    }

}
