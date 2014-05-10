package com.rethinkdb.response;

import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.List;

public class InsertResult implements DBResult {
    private int inserted;
    private int replaced;
    private int unchanged;
    private int errors;
    private String first_error;
    private int deleted;
    private int skipped;
    private List<String> generated_keys;
    private DBObject old_val;
    private DBObject new_val;

    public InsertResult(Q2L.Response response) {
        Q2L.Datum datum = response.getResponse(0);
        DBObject object = DBObjectMapper.fromDatum(datum);
        DBObjectMapper.populateObject(this, object);
    }

    @Override
    public String toString() {
        return "InsertResult{" +
                "inserted=" + inserted +
                ", replaced=" + replaced +
                ", unchanged=" + unchanged +
                ", errors=" + errors +
                ", first_error='" + first_error + '\'' +
                ", deleted=" + deleted +
                ", skipped=" + skipped +
                ", generated_keys=" + generated_keys +
                ", old_val=" + old_val +
                ", new_val=" + new_val +
                '}';
    }
}
