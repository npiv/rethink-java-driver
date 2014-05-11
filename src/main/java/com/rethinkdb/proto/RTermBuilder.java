package com.rethinkdb.proto;

import com.rethinkdb.model.DBObject;

import static com.rethinkdb.proto.Q2L.Term;

/**
 * Facilitate creation of ProtoBuf Term objects
 */
public class RTermBuilder {

    private Term.Builder term = Term.newBuilder();

    public RTermBuilder ofType(Term.TermType type) {
        term.setType(type);
        return this;
    }

    public RTermBuilder addArg(Term... args) {
        for (Term arg : args) {
            term.addArgs(arg);
        }
        return this;
    }

    public Term build() {
        return term.build();
    }

    public static Term dbTerm(String dbName) {
        return Q2L.Term.newBuilder().setType(Q2L.Term.TermType.DB)
                .addArgs(RTermBuilder.datumTerm(dbName)).build();
    }

    public static Term datumTerm(String payload) {
        return createDatumTerm(payload);
    }

    public static Term datumTerm(Number payload) {
        return createDatumTerm(payload);
    }

    public static Term datumTerm(boolean payload) {
        return createDatumTerm(payload);
    }

    public static Term datumTerm(DBObject payload) {
        return createDatumTerm(payload);
    }

    protected static Term createDatumTerm(Object value) {
        return Term.newBuilder()
                .setType(Term.TermType.DATUM)
                .setDatum(RDatumBuilder.createDatum(value))
                .build();
    }

    public static Term tableTerm(String tableName) {
        return new RTermBuilder()
                .ofType(Q2L.Term.TermType.TABLE)
                .addArg(RTermBuilder.datumTerm(tableName))
                .build();
    }
}
