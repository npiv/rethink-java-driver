package com.rethinkdb.query;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

import java.util.Map;

import static com.rethinkdb.proto.Q2L.*;

public class TermBuilder {

    private Term.Builder term = Term.newBuilder();

    public TermBuilder ofType(Term.TermType type) {
        term.setType(type);
        return this;
    }

    public TermBuilder addArg(Term... args) {
        for (Term arg : args) {
             term.addArgs(arg);
        }
        return this;
    }

    public Term build() {
        return term.build();
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
                .setDatum(createDatum(value))
                .build();
    }

    private static Datum createDatum(Object value) {
        Datum.Builder builder = Datum.newBuilder();

        if (value instanceof String) {
            return builder
                    .setType(Datum.DatumType.R_STR)
                    .setRStr((String)value)
                    .build();
        }

        if (value instanceof Number) {
            return builder
                    .setType(Datum.DatumType.R_NUM)
                    .setRNum(((Number)value).doubleValue())
                    .build();
        }

        if (value instanceof Boolean) {
            return builder
                    .setType(Datum.DatumType.R_BOOL)
                    .setRBool((Boolean) value)
                    .build();
        }

        if (value instanceof DBObject) {
            return createDatum((DBObject) value);
        }

        throw new RethinkDBException("Unknown Value can't create datatype for : " + value.getClass());
    }

    private static Q2L.Datum createDatum(DBObject parameter) {
        Q2L.Datum.Builder builder = Q2L.Datum.newBuilder()
                .setType(Q2L.Datum.DatumType.R_OBJECT);

        for (Map.Entry<String, Object> kv : parameter.getAsMap().entrySet()) {
            builder.addRObject(Q2L.Datum.AssocPair.newBuilder()
                            .setKey(kv.getKey())
                            .setVal(createDatum(kv.getValue()))
                            .build()
            );
        }

        return builder.build();
    }

}
