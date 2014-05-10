package com.rethinkdb.mapper;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.proto.Q2L;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBObjectMapper {

    public static DBObject fromDatum(Q2L.Datum datum) {
        if (datum.getType() != Q2L.Datum.DatumType.R_OBJECT) {
            throw new RethinkDBException("Can't map a non object datum to a DBObject. Got : " + datum.getType());
        }

        Map<String, Object> repr = new HashMap<String, Object>();
        for (Q2L.Datum.AssocPair assocPair : datum.getRObjectList()) {
            repr.put(assocPair.getKey(), handleType(assocPair.getVal()));
        }
        return DBObject.fromMap(repr);
    }

    private static Object handleType(Q2L.Datum val) {
        switch (val.getType()) {
            case R_OBJECT: return fromDatum(val);
            case R_STR: return val.getRStr();
            case R_BOOL: return val.getRBool();
            case R_NULL: return null;
            case R_NUM: return val.getRNum();
            case R_ARRAY: return makeArray(val.getRArrayList());
            case R_JSON:
            default: throw new RuntimeException("Not implemented" + val.getType());
        }
    }

    private static List<Object> makeArray(List<Q2L.Datum> pairs) {
        List<Object> objects = new ArrayList<Object>();
        for (Q2L.Datum pair : pairs) {
            objects.add(handleType(pair));
        }
        return objects;
    }

    public static <T> void populateObject(T to, DBObject from) {
        for (Field field : to.getClass().getDeclaredFields()){
            try {
                field.setAccessible(true);

                if (field.getType().equals(Integer.class) || field.getType().equals(int.class)) {
                    field.set(to, ((Double)from.getAsMap().get(field.getName())).intValue());
                }
                else {
                    field.set(to, from.getAsMap().get(field.getName()));
                }

            } catch (IllegalAccessException e) {
                throw new RethinkDBException("Error populating from DBObject", e);
            }
        }
    }
}
