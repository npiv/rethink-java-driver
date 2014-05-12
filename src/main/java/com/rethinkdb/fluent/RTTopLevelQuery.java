package com.rethinkdb.fluent;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.ast.RTOperationConverter;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RTTopLevelQuery<T> {
    protected static final Logger logger = LoggerFactory.getLogger(RTFluentQuery.class);

    protected Class<T> sampleClass;
    public RTTreeKeeper treeKeeper;

    protected RTTopLevelQuery(RTTreeKeeper treeKeeper, Class<T> sampleClass) {
        this.treeKeeper = treeKeeper;
        this.sampleClass = sampleClass;
    }

    protected RTTopLevelQuery() {
        this.treeKeeper = new RTTreeKeeper();
    }

    public T run(RethinkDBConnection connection) {
        logger.debug("ready to run for {}", treeKeeper.getTree());
        DBObject dbObject = connection.run(RTOperationConverter.toProtoBufTerm(treeKeeper.getTree()));

        try {
            if (sampleClass.equals(DBObject.class)) {
                return (T) dbObject;
            } else if (sampleClass.equals(List.class)) {
                return DBObjectMapper.populateList(dbObject);
            } else {
                return DBObjectMapper.populateObject(sampleClass.newInstance(), dbObject);
            }
        } catch (InstantiationException e) {
            throw new RethinkDBException("DBResult must have default constructor " + sampleClass, e);
        } catch (IllegalAccessException e) {
            throw new RethinkDBException("DBResult must have default constructor " + sampleClass, e);
        }
    }
}
