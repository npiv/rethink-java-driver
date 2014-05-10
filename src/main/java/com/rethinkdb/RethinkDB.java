package com.rethinkdb;

import com.rethinkdb.query.RQLQueryBuilder;

public class RethinkDB extends RQLQueryBuilder {

    public static RethinkDB r = new RethinkDB();

    private RethinkDB() {}

    public RethinkDBConnection connect() {
        return new RethinkDBConnection();
    }

    public RethinkDBConnection connect(String hostname) {
        return new RethinkDBConnection(hostname);
    }

    public RethinkDBConnection connect(String hostname, int port) {
        return new RethinkDBConnection(hostname, port);
    }

    public RethinkDBConnection connect(String hostname, int port, String authKey) {
        return new RethinkDBConnection(hostname, port, authKey);
    }

    public RethinkDBConnection connect(String hostname, String authKey) {
        return new RethinkDBConnection(hostname, authKey);
    }

}
