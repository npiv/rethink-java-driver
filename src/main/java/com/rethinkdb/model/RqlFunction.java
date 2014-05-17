package com.rethinkdb.model;

import com.rethinkdb.ast.query.RqlQuery;

public interface RqlFunction {
    RqlQuery apply(RqlQuery row);
}
