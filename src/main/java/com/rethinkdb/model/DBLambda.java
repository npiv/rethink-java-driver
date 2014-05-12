package com.rethinkdb.model;

import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTFluentRow;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public abstract class DBLambda {
    public abstract RTFluentRow apply(RTFluentRow row);

    public RTOperation getOperation() {
        List<Object> params = new ArrayList<Object>();
        params.add(1);

        return new RTOperation(Q2L.Term.TermType.FUNC)
                .withArgs(
                        params,
                        apply(new RTFluentRow()).treeKeeper.getTree()
                );

    }
}
