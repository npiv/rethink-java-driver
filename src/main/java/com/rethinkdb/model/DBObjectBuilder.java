package com.rethinkdb.model;

import java.util.List;
import java.util.Map;

public class DBObjectBuilder {
    private DBObject object = new DBObject();

    public DBObjectBuilder() {}

    private DBObjectBuilder(Map<String, Object> map) {
        object = new DBObject(map);
    }

    public static DBObject buildFromMap(Map<String, Object> map) {
        return new DBObjectBuilder(map).build();
    }

    public DBObjectBuilder with(String key, Number value) {
        object.map.put(key, value);
        return this;
    }

    public DBObjectBuilder with(String key, String value) {
        object.map.put(key, value);
        return this;
    }

    public <T> DBObjectBuilder with(String key, List<T> value) {
        object.map.put(key, value);
        return this;
    }

    public DBObject build() {
        return object;
    }
}
