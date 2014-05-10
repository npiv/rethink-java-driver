package com.rethinkdb.model;

import java.util.HashMap;
import java.util.Map;

public class DBObject {
    private Map<String, Object> map;

    private DBObject() {
        map = new HashMap<String, Object>();
    }

    private DBObject(Map<String, Object> map) {
        this.map = map;
    }

    public static DBObject fromMap(Map<String, Object> map) {
        return new DBObject(map);
    }

    public static DBObject build() {
        return new DBObject();
    }

    public DBObject with(String key, Number value) {
        map.put(key,value);
        return this;
    }

    public DBObject with(String key, String value) {
        map.put(key,value);
        return this;
    }

    public Map<String, Object> getAsMap() {
        return map;
    }

    @Override
    public String toString() {
        return "DBObject{"  + map + '}';
    }
}
