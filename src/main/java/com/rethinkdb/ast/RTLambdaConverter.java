package com.rethinkdb.ast;

import com.rethinkdb.fluent.RTFluentRow;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public class RTLambdaConverter {
    public static RTOperation getOperation(DBLambda lambda) {
        List<Object> params = new ArrayList<Object>();
        params.add(1);

        return new RTOperation(Q2L.Term.TermType.FUNC)
                .withArgs(
                        params,
                        lambda.apply(new RTFluentRow()).treeKeeper.getTree()
                );

    }
}
