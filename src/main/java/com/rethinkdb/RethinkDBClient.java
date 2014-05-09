package com.rethinkdb;

import com.rethinkdb.error.RethinkDBConnectException;
import com.rethinkdb.proto.Q2L;

public class RethinkDBClient {

    private String hostname;
    private int port;
    private String authKey;

    private SocketChannelFacade socket = new SocketChannelFacade();

    public RethinkDBClient() {
        this("localhost", "");
    }

    public RethinkDBClient(String hostname) {
        this(hostname, "");
    }

    public RethinkDBClient(String hostname, String authKey) {
        this(hostname, 28015, authKey);
    }

    public RethinkDBClient(String hostname, int port, String authKey) {
        this.hostname = hostname;
        this.port = port;
        this.authKey = authKey;

        reconnect();
    }

    private void reconnect() {
        socket.connect(hostname, port);
        socket.writeLEInt(Q2L.VersionDummy.Version.V0_2.getNumber());
        socket.writeStringWithLength(authKey);

        String result = socket.readString();
        if (!result.startsWith("SUCCESS")) {
            throw new RethinkDBConnectException(result);
        }

        Q2L.Query.Builder builder = Q2L.Query.newBuilder();
        builder.setType(Q2L.Query.QueryType.START);
        builder.setToken(5);
        builder.setQuery(
                Q2L.Term.newBuilder()
                        .setType(Q2L.Term.TermType.DB_CREATE)
                        .addArgs(
                                Q2L.Term.newBuilder()
                                        .setType(Q2L.Term.TermType.DATUM)
                                        .setDatum(
                                                Q2L.Datum.newBuilder()
                                                        .setType(Q2L.Datum.DatumType.R_STR)
                                                        .setRStr("testTable")
                                        )
                        )
        );

        socket.write(builder.build().toByteArray());
        Q2L.Response response = socket.read();
        System.out.println(response);
    }

    public static void main(String[] args) {
        new RethinkDBClient("agillo.net");
    }
}
