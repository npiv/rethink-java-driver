package com.rethinkdb.proto;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.DBObject;

import java.util.List;
import java.util.Map;

/**
 * Facilitate creation of ProtoBuf Datum objects
 */
public class RDatumBuilder {
    private RDatumBuilder() {}

    public static Q2L.Datum createDatum(Object value) {
        Q2L.Datum.Builder builder = Q2L.Datum.newBuilder();

        if (value == null) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_NULL)
                    .build();
        }

        if (value instanceof String) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_STR)
                    .setRStr((String) value)
                    .build();
        }

        if (value instanceof Number) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_NUM)
                    .setRNum(((Number) value).doubleValue())
                    .build();
        }

        if (value instanceof Boolean) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_BOOL)
                    .setRBool((Boolean) value)
                    .build();
        }

        if (value instanceof DBObject) {
            return createDatumFromDBObject((DBObject) value);
        }

        throw new RethinkDBException("Unknown Value can't create datatype for : " + value.getClass());
    }

    private static Q2L.Datum createDatumFromDBObject(DBObject parameter) {
        Q2L.Datum.Builder builder = Q2L.Datum.newBuilder()
                .setType(Q2L.Datum.DatumType.R_OBJECT);

        for (Map.Entry<String, Object> kv : parameter.getAsMap().entrySet()) {
            builder.addRObject(RAssocPairBuilder.datumPair(kv.getKey(), kv.getValue()));
        }

        return builder.build();
    }
}
