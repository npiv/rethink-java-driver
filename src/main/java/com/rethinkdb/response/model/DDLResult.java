package com.rethinkdb.response.model;

public class DDLResult {
    private int created;
    private int dropped;

    /**
     * The number of created entities
     */
    public int getCreated() {
        return created;
    }

    /**
     * The number of dropped entities
     */
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
