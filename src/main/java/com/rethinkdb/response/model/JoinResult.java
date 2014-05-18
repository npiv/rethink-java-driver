package com.rethinkdb.response.model;

import java.util.Map;

public class JoinResult {
    private Map<String, Object> left;
    private Map<String, Object> right;

    public Map<String, Object> getLeft() {
        return left;
    }

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
