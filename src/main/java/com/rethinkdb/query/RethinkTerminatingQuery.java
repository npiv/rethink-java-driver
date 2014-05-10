package com.rethinkdb.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResult;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A query that has no further actions available upon it except run
 *
 * This object is created by RethinkQueryBuilder when the query is complete
 */
public class RethinkTerminatingQuery<T extends DBResult> {
    private static final AtomicInteger tokenFactory = new AtomicInteger();
    private final Class<? extends DBResult> resultClazz;

    private QueryInformation queryInformation;

    public RethinkTerminatingQuery(
            Class<? extends DBResult> resultClazz,
            QueryInformation queryInformation
    ) {
        this.resultClazz = resultClazz;
        this.queryInformation = queryInformation;
    }

    /**
     * run the query on the given connection
     * @param connection connection
     * @return DBResult
     */
    // TODO: find a place to define the type of DBResult specifically
    public T run(RethinkDBConnection connection) {
        Q2L.Query.Builder query = Q2L.Query.newBuilder()
                .setType(queryInformation.getQueryType())
                .setToken(tokenFactory.incrementAndGet())
                .setQuery(queryForTermType());

        DBObject dbObject = connection.run(query);
        try {
            if (resultClazz.equals(DBObject.class)) {
                return (T)dbObject;
            }
            else {
                return (T) DBObjectMapper.populateObject(resultClazz.newInstance(), dbObject);
            }
        } catch (InstantiationException e) {
            throw new RethinkDBException("DBResult must have default constructor " + resultClazz, e);
        } catch (IllegalAccessException e) {
            throw new RethinkDBException("DBResult must have default constructor " + resultClazz, e);
        }
    }

    private Q2L.Term queryForTermType() {
        return Q2L.Term.newBuilder()
                .setType(queryInformation.getTermType())
                .addAllArgs(queryInformation.getArgs())
                .addAllOptargs(queryInformation.getOptsArgs())
                .build();
    }

}
