package com.rethinkdb.fluent;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.RTOperationConverter;
import com.rethinkdb.ast.RTTreeKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RTTopLevelQuery<T> {
    protected static final Logger logger = LoggerFactory.getLogger(RTFluentQuery.class);

    public RTTreeKeeper treeKeeper;

    protected RTTopLevelQuery(RTTreeKeeper treeKeeper) {
        this.treeKeeper = treeKeeper;
    }

    protected RTTopLevelQuery() {
        this.treeKeeper = new RTTreeKeeper();
    }

    public Object run(RethinkDBConnection connection) {
        logger.debug("ready to run for {}", treeKeeper.getTree());
        return connection.run(RTOperationConverter.toProtoBufTerm(treeKeeper.getTree()));
    }
}
