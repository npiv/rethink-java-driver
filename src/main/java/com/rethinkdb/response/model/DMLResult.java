package com.rethinkdb.response.model;

import com.rethinkdb.model.DBObject;

import java.util.List;

public class DMLResult {
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

    /**
     *  the number of documents that were succesfully inserted.
     */
    public int getInserted() {
        return inserted;
    }

    /**
     * the number of documents that were updated when upsert is used.
     */
    public int getReplaced() {
        return replaced;
    }

    /**
     * the number of documents that would have been modified, except that the new value was the same as the old value when doing an upsert.
     */
    public int getUnchanged() {
        return unchanged;
    }

    /**
     * the number of errors encountered while performing the insert.
     */
    public int getErrors() {
        return errors;
    }

    /**
     * if errors were encountered, contains the text of the first error.
     */
    public String getFirst_error() {
        return first_error;
    }

    /**
     * always 0 for an insert operation
     */
    public int getDeleted() {
        return deleted;
    }

    /**
     * always 0 for an insert operation
     */
    public int getSkipped() {
        return skipped;
    }

    /**
     * a list of generated primary keys in case the primary keys for some documents were missing (capped to 100000).
     */
    public List<String> getGenerated_keys() {
        return generated_keys;
    }

    /**
     * if returnVals is set to true, contains null.
     * @return old value
     */
    public DBObject getOld_val() {
        return old_val;
    }

    /**
     * if returnVals is set to true, contains the inserted/updated document.
     * @return new value
     */
    public DBObject getNew_val() {
        return new_val;
    }
}
