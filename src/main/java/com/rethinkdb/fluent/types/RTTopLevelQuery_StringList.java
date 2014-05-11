package com.rethinkdb.fluent.types;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.ast.RTOperationConverter;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTTopLevelQuery;
import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RTTopLevelQuery_StringList extends RTTopLevelQuery<List> {

    public RTTopLevelQuery_StringList(RTTreeKeeper treeKeeper) {
        super(treeKeeper, List.class);
    }

    @Override
    public List<String> run(RethinkDBConnection connection) {
        return super.run(connection);
    }
}
