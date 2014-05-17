package com.rethinkdb.ast.helper;

import java.util.HashMap;
import java.util.List;

public class OptionalArguments extends HashMap<String, Object> {

    public OptionalArguments() {
        super();
    }

    public OptionalArguments with(String key, Object value) {
        if (value != null) {
            put(key, value);
        }
        return this;
    }

    public OptionalArguments with(String key, List<Object> value) {
        if (value != null) {
            put(key, value);
        }
        return this;
    }

}
