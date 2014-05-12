package com.rethinkdb.model;

import com.rethinkdb.fluent.RTFluentRow;

public interface DBLambda {
    RTFluentRow apply(RTFluentRow row);
}
