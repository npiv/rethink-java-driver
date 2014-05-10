package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.proto.Q2L;

public class DMLResult implements DBResult {
    private int created;
    private int dropped;

    public DMLResult(Q2L.Response response) {
        Q2L.Datum datum = response.getResponse(0);
        for (Q2L.Datum.AssocPair assocPair : datum.getRObjectList()) {
            if (assocPair.getKey().equals("created")) {
                created = new Double(assocPair.getVal().getRNum()).intValue();
                continue;
            }
            if (assocPair.getKey().equals("dropped")) {
                dropped = new Double(assocPair.getVal().getRNum()).intValue();
                continue;
            }

            throw new RethinkDBException("Found unknown result: " + assocPair.getKey());
        }
    }


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
