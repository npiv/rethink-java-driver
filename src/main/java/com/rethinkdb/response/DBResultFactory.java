package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.proto.Q2L;

public class DBResultFactory {

    private DBResultFactory() {
    }

    public static <T> T convert(Q2L.Response response) {
        switch (response.getType()) {
            case SUCCESS_ATOM:
                return DBResponseMapper.fromDatumObject(response.getResponse(0));
            case SUCCESS_SEQUENCE:
                return (T) DBResponseMapper.fromDatumObjectList(response.getResponseList());

            case WAIT_COMPLETE:
            case SUCCESS_PARTIAL:
                throw new UnsupportedOperationException();

            case CLIENT_ERROR:
            case COMPILE_ERROR:
            case RUNTIME_ERROR:
                throw new RethinkDBException(response.getType() + ": " + response.getResponse(0).getRStr());

            default:
                throw new RethinkDBException("Unknown Response Type: " + response.getType());
        }
    }
}
