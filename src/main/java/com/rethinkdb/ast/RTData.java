package com.rethinkdb.ast;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

public class RTData<T> extends RTOperation {

    private T value;

    public RTData(T value) {
        super(Q2L.Term.TermType.DATUM);
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
