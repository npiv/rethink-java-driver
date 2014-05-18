package com.rethinkdb.model;

import com.rethinkdb.ast.query.RqlQuery;

public interface RqlFunction extends RqlLambda {
    RqlQuery apply(RqlQuery row);
}
