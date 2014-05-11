package com.rethinkdb.proto;

/**
 * Facilitate creation of ProtoBuf AssocPair objects
 */
public class RAssocPairBuilder {
    private RAssocPairBuilder() {
    }

    public static Q2L.Datum.AssocPair datumPair(String key, Object value) {
        return Q2L.Datum.AssocPair.newBuilder()
                .setKey(key)
                .setVal(RDatumBuilder.createDatum(value))
                .build();

    }

    public static Q2L.Term.AssocPair termPair(String key, Q2L.Term value) {
        return Q2L.Term.AssocPair.newBuilder()
                .setKey(key)
                .setVal(value)
                .build();

    }

    public static Q2L.Query.AssocPair queryPair(String key, Q2L.Term term) {
        return Q2L.Query.AssocPair.newBuilder()
                .setKey(key)
                .setVal(term)
                .build();
    }
}
