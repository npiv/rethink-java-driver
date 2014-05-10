package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.proto.Q2L;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

public interface DBResult {

    public enum ExpectedDBResult {
        StringList, Insert, Generic
    }

}
