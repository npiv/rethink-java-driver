package com.rethinkdb.ast;

import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RAssocPairBuilder;
import com.rethinkdb.proto.RDatumBuilder;
import com.rethinkdb.proto.RTermBuilder;

import java.util.Map;

public class RTOperationConverter {

    public static Q2L.Term toProtoBufTerm(RTOperation operation) {
        Q2L.Term.Builder builder = Q2L.Term.newBuilder();
        builder.setType(operation.getTermType());

        if (operation instanceof RTData) {
            builder.setDatum(RDatumBuilder.createDatum(((RTData) operation).getValue()));
        }

        for (Object o : operation.getArgs()) {
            if (o instanceof RTOperation) {
                builder.addArgs(toProtoBufTerm((RTOperation) o));
            } else {
                builder.addArgs(RTermBuilder.createDatumTerm(o));
            }
        }

        for (Map.Entry<String, Object> kv : operation.getOptionalArgs().entrySet()) {
            builder.addOptargs(
                    RAssocPairBuilder.termPair(
                            kv.getKey(),
                            RTermBuilder.createDatumTerm(kv.getValue())
                    )
            );
        }

        return builder.build();
    }

}
