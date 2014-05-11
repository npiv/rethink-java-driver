package com.rethinkdb.query;

public class DataResultQuery<T> extends RethinkTerminatingQuery<T> {

    public DataResultQuery(Class<T> resultClazz, QueryInformation queryInformation) {
        super(resultClazz, queryInformation);
    }
}
