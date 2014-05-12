package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.proto.Q2L;

public class RTFluentRow extends RTFluentQuery {

    public RTFluentRow() {
    }

    public RTFluentRow(RTTreeKeeper treeKeeper, Class sampleClass) {
        super(treeKeeper, sampleClass);
    }

    public RTFluentRow add(double addition) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.ADD).withArgs(addition);
        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
    }

    public RTFluentRow field(String fieldName) {
        RTOperation operation = new RTOperation(Q2L.Term.TermType.GET_FIELD)
                .withArgs(new RTOperation(Q2L.Term.TermType.VAR).withArgs(1))
                .withArgs(fieldName);

        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
    }



}
