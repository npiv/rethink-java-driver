package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.mapper.DBObjectMapper;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

public class DBResultFactory {

    private DBResultFactory() {
    }

    public static DBObject convert(Q2L.Response response) {
        switch (response.getType()) {
            case SUCCESS_ATOM:
                return DBObjectMapper.fromDatumObject(response.getResponse(0));
            case SUCCESS_SEQUENCE:
                return DBObjectMapper.fromDatumObjectList(response.getResponseList());

            case WAIT_COMPLETE:
            case SUCCESS_PARTIAL:
                throw new UnsupportedOperationException();

            case CLIENT_ERROR:
            case COMPILE_ERROR:
            case RUNTIME_ERROR:
                throw new RethinkDBException(response.getResponse(0).getRStr());

            default:
                throw new RethinkDBException("Unknown Response Type: " + response.getType());
        }
    }
}
