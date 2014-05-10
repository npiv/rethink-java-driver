package com.rethinkdb;

import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResult;
import com.rethinkdb.response.DBResultFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class RethinkDBConnection {

    private final AtomicInteger token = new AtomicInteger();

    private String hostname;
    private String authKey;
    private int port;
    private String dbName;

    private SocketChannelFacade socket = new SocketChannelFacade();

    public RethinkDBConnection() {
        this("localhost", "");
    }

    public RethinkDBConnection(String hostname) {
        this(hostname, "");
    }

    public RethinkDBConnection(String hostname, int port) {
        this(hostname, port, "");
    }

    public RethinkDBConnection(String hostname, String authKey) {
        this(hostname, 28015, authKey);
    }

    public RethinkDBConnection(String hostname, int port, String authKey) {
        this.hostname = hostname;
        this.port = port;
        this.authKey = authKey;
        this.dbName = "test";
        reconnect();
    }

    public void reconnect() {
        socket.connect(hostname, port);
        socket.writeLEInt(Q2L.VersionDummy.Version.V0_2.getNumber());
        socket.writeStringWithLength(authKey);

        String result = socket.readString();
        if (!result.startsWith("SUCCESS")) {
            throw new RethinkDBException(result);
        }
    }

    public void use(String dbName) {
        this.dbName = dbName;
    }

    // TODO: this needs to be protected
    public DBResult run(Q2L.Query query, DBResult.ExpectedDBResult expectedDBResult) {
        socket.write(query.toByteArray());
        Q2L.Response response = socket.read();
        return DBResultFactory.convert(response, expectedDBResult);
    }
}
