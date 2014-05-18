package com.rethinkdb.response.model;

public class IndexStatusResult {
    private String index;
    private boolean ready;

    /**
     * the index name
     */
    public String getIndex() {
        return index;
    }

    /**
     * is index ready
     */
    public boolean isReady() {
        return ready;
    }

    @Override
    public String toString() {
        return "IndexStatusResult{" +
                "index='" + index + '\'' +
                ", ready=" + ready +
                '}';
    }
}
