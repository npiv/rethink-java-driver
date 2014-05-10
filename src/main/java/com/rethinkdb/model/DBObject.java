package com.rethinkdb.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
