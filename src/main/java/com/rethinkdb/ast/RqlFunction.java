package com.rethinkdb.ast;

import com.rethinkdb.ast.query.RqlQuery;

public interface RqlFunction {
    RqlQuery apply(RqlQuery row);
}
