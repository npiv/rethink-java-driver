package com.rethinkdb.response;

import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public class StringListDBResult implements DBResult {
    private List<String> list = new ArrayList<String>();

    public List<String> getResult() {
        return list;
    }

    @Override
    public String toString() {
        return "StringListDBResult{" + list + '}';
    }
}
