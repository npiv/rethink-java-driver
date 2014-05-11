package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.fluent.RTTopLevelQuery;
import com.rethinkdb.model.DBObject;

import java.util.List;

public class RTFluentQuery_DBObjectList extends RTFluentQuery<List> {

    public RTFluentQuery_DBObjectList(RTTreeKeeper treeKeeper) {
        super(treeKeeper, List.class);
    }

    @Override
    public List<DBObject> run(RethinkDBConnection connection) {
        return super.run(connection);
    }
}
