package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.model.DBObject;

import java.util.ArrayList;
import java.util.List;

public class RTFluentQuery_ObjectList<T> extends RTFluentQuery<List> {

    public RTFluentQuery_ObjectList(RTTreeKeeper treeKeeper) {
        super(treeKeeper, List.class);
    }

    @Override
    public List<T> run(RethinkDBConnection connection) {
        List<DBObject> dbObjectResult = super.run(connection);
        List<T> result = new ArrayList<T>();
        for (DBObject dbObject : dbObjectResult) {
            result.add((T)dbObject.get(DBObject.VALUE));
        }
        return result;
    }
}
