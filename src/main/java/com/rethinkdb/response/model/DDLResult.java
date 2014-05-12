package com.rethinkdb.response.model;

public class DDLResult {
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
