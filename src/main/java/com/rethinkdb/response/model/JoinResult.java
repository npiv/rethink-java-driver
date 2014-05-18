package com.rethinkdb.response.model;

import java.util.Map;

public class JoinResult {
    private Map<String, Object> left;
    private Map<String, Object> right;

    /**
     * The left object in the join
     * @return left object
     */
    public Map<String, Object> getLeft() {
        return left;
    }

    /**
     * The right object in the join
     * @return right object
     */
    public Map<String, Object> getRight() {
        return right;
    }

    @Override
    public String toString() {
        return "JoinResult{" +
                "left=" + left +
                ", right=" + right +
                '}';
    }
}
