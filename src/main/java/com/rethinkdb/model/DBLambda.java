package com.rethinkdb.model;

import com.rethinkdb.fluent.RTFluentQuery;

public interface DBLambda {
    RTFluentQuery apply(RTFluentQuery row);
}
