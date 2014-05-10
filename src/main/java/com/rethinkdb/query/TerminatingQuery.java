package com.rethinkdb.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResult;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A query that has no further actions available upon it except
 * run
 */
public class TerminatingQuery {
    private static final AtomicInteger tokenFactory = new AtomicInteger();

    private DBResult.ExpectedDBResult expectedDBResult;
    private QueryInformation queryInformation;

    public TerminatingQuery(DBResult.ExpectedDBResult expectedDBResult, QueryInformation queryInformation) {
        this.expectedDBResult = expectedDBResult;
        this.queryInformation = queryInformation;
    }

    public com.rethinkdb.response.DBResult run(RethinkDBConnection connection) {
        Q2L.Query query = Q2L.Query.newBuilder()
                .setType(queryInformation.getQueryType())
                .setToken(tokenFactory.incrementAndGet())
                .setQuery(queryForTermType())
                .build();

        return connection.run(query, expectedDBResult);
    }

    private Q2L.Term queryForTermType() {
        return Q2L.Term.newBuilder()
                .setType(queryInformation.getTermType())
                .addAllArgs(queryInformation.getArgs())
                .addAllOptargs(queryInformation.getOptsArgs())
                .build();
    }

}
