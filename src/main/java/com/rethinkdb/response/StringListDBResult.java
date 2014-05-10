package com.rethinkdb.response;

import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public class StringListDBResult implements DBResult {
    private List<String> result = new ArrayList<String>();

    public StringListDBResult(Q2L.Response response) {
        for (Q2L.Datum datum : response.getResponse(0).getRArrayList()) {
            result.add(datum.getRStr());
        }
    }

    public List<String> getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "StringListDBResult{" + result + '}';
    }
}
