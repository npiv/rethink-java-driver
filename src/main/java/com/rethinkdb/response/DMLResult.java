package com.rethinkdb.response;

public class DMLResult implements DBResult {
    private int created;
    private int dropped;

    public int getCreated() {
        return created;
    }

    public int getDropped() {
        return dropped;
    }

    @Override
    public String toString() {
        return "GenericDBResult{" +
                "created=" + created +
                ", dropped=" + dropped +
                "}";
    }
}
