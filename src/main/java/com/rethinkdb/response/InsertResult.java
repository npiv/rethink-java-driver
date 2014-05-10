package com.rethinkdb.response;

import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

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

    @Override
    public String toString() {
        return "InsertResult{" +
                "inserted=" + inserted +
                ", replaced=" + replaced +
                ", unchanged=" + unchanged +
                ", errors=" + errors +
                ", first_error=" + first_error +
                ", deleted=" + deleted +
                ", skipped=" + skipped +
                ", generated_keys=" + generated_keys +
                ", old_val=" + old_val +
                ", new_val=" + new_val +
                '}';
    }

    public int getInserted() {
        return inserted;
    }

    public int getReplaced() {
        return replaced;
    }

    public int getUnchanged() {
        return unchanged;
    }

    public int getErrors() {
        return errors;
    }

    public String getFirst_error() {
        return first_error;
    }

    public int getDeleted() {
        return deleted;
    }

    public int getSkipped() {
        return skipped;
    }

    public List<String> getGenerated_keys() {
        return generated_keys;
    }

    public DBObject getOld_val() {
        return old_val;
    }

    public DBObject getNew_val() {
        return new_val;
    }
}
