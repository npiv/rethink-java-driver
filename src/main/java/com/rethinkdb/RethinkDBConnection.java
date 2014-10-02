package com.rethinkdb;

import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResultFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RethinkDBConnection {
    private static final Logger logger = LoggerFactory.getLogger(RethinkDBConnection.class);

    private static final AtomicInteger tokenGenerator = new AtomicInteger();

    private String hostname;
    private String authKey;
    private int port;
    private int timeout;
    private String dbName = null;

    private SocketChannelFacade socket = new SocketChannelFacade();

    public RethinkDBConnection() {
        this(RethinkDBConstants.DEFAULT_HOSTNAME);
    }

    public RethinkDBConnection(String hostname) {
        this(hostname, RethinkDBConstants.DEFAULT_PORT);
    }

    public RethinkDBConnection(String hostname, int port) {
        this(hostname, port, "");
    }

    public RethinkDBConnection(String hostname, int port, String authKey) {
        this(hostname, port, authKey, RethinkDBConstants.DEFAULT_TIMEOUT);
    }

    public RethinkDBConnection(String hostname, int port, String authKey, int timeout) {
        this.hostname = hostname;
        this.port = port;
        this.authKey = authKey;
        this.timeout = timeout;
        reconnect();
    }

    // TODO: use timeout
    public void reconnect() {
        socket.connect(hostname, port);
        socket.writeLEInt(Q2L.VersionDummy.Version.V0_2.getNumber());
        socket.writeStringWithLength(authKey);

        String result = socket.readString();
        if (!result.startsWith("SUCCESS")) {
            throw new RethinkDBException(result);
        }
    }

    // TODO: When partial implemented add option to wait for jobs to finish
    public void close() {
        socket.close();
    }

    public boolean isClosed(){
        return socket.isClosed();
    }

    public void use(String dbName) {
        this.dbName = dbName;
    }


    protected Q2L.Response getNext(long token) {
        Q2L.Query query = Q2L.Query
                .newBuilder()
                .setToken(token)
                .setType(Q2L.Query.QueryType.CONTINUE)
                .build();
        return execute(query);
    }

    protected <T> void closeCursor(Cursor<T> cursor) {
        Q2L.Query query = Q2L.Query
                .newBuilder()
                .setToken(cursor.getToken())
                .setType(Q2L.Query.QueryType.CONTINUE)
                .build();
        execute(query);
    }

    public <T> Cursor<T> runForCursor(Q2L.Term term) {

        Q2L.Query.Builder queryBuilder = startQuery(term);

        Q2L.Response response = execute(queryBuilder.build());

        return new Cursor<T>(this, response);
    }

    public <T> T run(Q2L.Term term) {
        Q2L.Query.Builder queryBuilder = startQuery(term);

        return DBResultFactory.convert(execute(queryBuilder.build()));
    }

    // set the global option dbName if user chose one through use
    private void setDbOptionIfNeeded(Q2L.Query.Builder q, String db) {
        if (db == null) return;

        if (!hasDBSet(q)) {
            q.addGlobalOptargs(
                    Q2L.Query.AssocPair.newBuilder()
                            .setKey("db")
                            .setVal(Q2L.Term.newBuilder()
                                    .setType(Q2L.Term.TermType.DB)
                                    .addArgs(Q2L.Term.newBuilder()
                                            .setType(Q2L.Term.TermType.DATUM)
                                            .setDatum(
                                                    Q2L.Datum.newBuilder()
                                                            .setType(Q2L.Datum.DatumType.R_STR)
                                                            .setRStr(db).build()
                                            )
                                    ).build())
            );
        }
    }

    private boolean hasDBSet(Q2L.Query.Builder q) {
        for (Q2L.Query.AssocPair assocPair : q.getGlobalOptargsList()) {
            if (assocPair.getKey().equals("db")) {
                return true;
            }
        }
        return false;
    }

    private Q2L.Query.Builder startQuery(Q2L.Term term) {
        Q2L.Query.Builder queryBuilder = Q2L.Query
                .newBuilder()
                .setToken(tokenGenerator.incrementAndGet())
                .setType(Q2L.Query.QueryType.START)
                .setQuery(term);

        setDbOptionIfNeeded(queryBuilder, this.dbName);

        return queryBuilder;
    }

    private <T> Q2L.Response execute(Q2L.Query query) {
        logger.debug("running {} ", query);
        socket.write(query.toByteArray());
        return socket.read();
    }

}
