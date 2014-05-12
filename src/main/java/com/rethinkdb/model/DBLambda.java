package com.rethinkdb.model;

import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.fluent.RTFluentRow;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public interface DBLambda {
    RTFluentRow apply(RTFluentRow row);


}
