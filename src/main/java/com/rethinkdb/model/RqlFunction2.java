package com.rethinkdb.model;

import com.rethinkdb.ast.query.RqlQuery;

public interface RqlFunction2  extends RqlLambda {
    RqlQuery apply(RqlQuery row1, RqlQuery row2);
}
