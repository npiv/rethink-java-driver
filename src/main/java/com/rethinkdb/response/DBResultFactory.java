package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.proto.Q2L;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class DBResultFactory {

    private DBResultFactory() {}

    public static DBResult convert(Q2L.Response response, DBResult.ExpectedDBResult expectedDBResult) {
        switch(response.getType()) {
            case SUCCESS_ATOM:
            case SUCCESS_SEQUENCE:
                return convertSuccess(response, expectedDBResult);

            case WAIT_COMPLETE:
            case SUCCESS_PARTIAL: throw new NotImplementedException();

            case CLIENT_ERROR:
            case COMPILE_ERROR:
            case RUNTIME_ERROR: throw new RethinkDBException(response.getResponse(0).getRStr());

            default: throw new RethinkDBException("Unknown Response Type: " + response.getType());
        }
    }

    private static DBResult convertSuccess(Q2L.Response response, DBResult.ExpectedDBResult expectedDBResult) {
        switch (expectedDBResult) {
            case Generic: return new DMLResult(response);
            case StringList: return new StringListDBResult(response);
            case Insert: return new InsertResult(response);

            default: throw new RethinkDBException("Unknown Expected Type: " + expectedDBResult);
        }
    }
}
