package com.rethinkdb.ast.query.gen;

import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.ast.query.RqlUtil;
import com.rethinkdb.proto.Q2L;

public class Datum extends RqlQuery {
    private Object value;

    public Datum(Object value) {
        super(Q2L.Term.TermType.DATUM);
        this.value = value;
    }

    @Override
    protected Q2L.Term toTerm() {
        return Q2L.Term.newBuilder().setType(this.getTermType()).setDatum(RqlUtil.createDatum(value)).build();
    }
}