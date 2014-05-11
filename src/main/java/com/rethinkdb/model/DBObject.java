package com.rethinkdb.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A low level representation of an Object that is stored or read from the database. This
 * is a more convenient interface than a raw map, but ultimately just stores a list of key
 * values.
 */
@SuppressWarnings("unchecked")
public class DBObject {
    public static final String CHILDREN = "list";

    protected Map<String, Object> map;

    protected DBObject() {
        map = new HashMap<String, Object>();
    }

    protected DBObject(Map<String, Object> map) {
        this.map = map;
    }

    public <T> T get(String key) {
        return (T) map.get(key);
    }

    public Map<String, Object> getAsMap() {
        return Collections.unmodifiableMap(map);
    }

    @Override
    public String toString() {
        return "DBObject{" + map + '}';
    }
}
