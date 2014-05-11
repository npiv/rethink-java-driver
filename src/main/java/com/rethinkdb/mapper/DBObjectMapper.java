package com.rethinkdb.mapper;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.proto.Q2L;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBObjectMapper {

    /**
     * Maps a ProtoBuf Datum object to a DBObject
     *
     * @param datum datum
     * @return DBObject
     */
    public static DBObject fromDatumObject(Q2L.Datum datum) {
        if (datum.getType() == Q2L.Datum.DatumType.R_OBJECT) {
            Map<String, Object> repr = new HashMap<String, Object>();
            for (Q2L.Datum.AssocPair assocPair : datum.getRObjectList()) {
                repr.put(assocPair.getKey(), handleType(assocPair.getVal()));
            }
            return DBObjectBuilder.buildFromMap(repr);
        }

        if (datum.getType() == Q2L.Datum.DatumType.R_ARRAY) {
            return new DBObjectBuilder().with(DBObject.CHILDREN, makeArray(datum.getRArrayList())).build();
        }

        throw new RethinkDBException("Can't map datum to a DBObject. Got : " + datum.getType());

    }

    /**
     * Maps a list of Datum Objects to a DBObject
     *
     * @param datums datum objects
     * @return DBObject
     */
    public static DBObject fromDatumObjects(List<Q2L.Datum> datums) {
        List<DBObject> results = new ArrayList<DBObject>();
        for (Q2L.Datum datum : datums) {
            results.add(fromDatumObject(datum));
        }
        return new DBObjectBuilder().with(DBObject.CHILDREN, results).build();
    }

    private static Object handleType(Q2L.Datum val) {
        switch (val.getType()) {
            case R_OBJECT:
                return fromDatumObject(val);
            case R_STR:
                return val.getRStr();
            case R_BOOL:
                return val.getRBool();
            case R_NULL:
                return null;
            case R_NUM:
                return val.getRNum();
            case R_ARRAY:
                return makeArray(val.getRArrayList());
            case R_JSON:
            default:
                throw new RuntimeException("Not implemented" + val.getType());
        }
    }

    private static List<Object> makeArray(List<Q2L.Datum> elements) {
        List<Object> objects = new ArrayList<Object>();
        for (Q2L.Datum element : elements) {
            objects.add(handleType(element));
        }
        return objects;
    }

    /**
     * Populates the fields in to based on values out of from
     *
     * @param to   the object to map into
     * @param from the object to map from
     * @param <T>  the type of the into object
     * @return the into object
     */
    public static <T> T populateObject(T to, DBObject from) {
        for (Field field : to.getClass().getDeclaredFields()) {
            try {
                field.setAccessible(true);

                Object result = convertField(field, from);
                if (result != null || !field.getType().isPrimitive()) {
                    field.set(to, result);
                }

            } catch (IllegalAccessException e) {
                throw new RethinkDBException("Error populating from DBObject: " + field.getName(), e);
            }
        }
        return to;
    }

    private static Object convertField(Field toField, DBObject from) {
        if (from.get(toField.getName()) == null) {
            return null;
        }
        if (toField.getType().equals(Integer.class) || toField.getType().equals(int.class)) {
            return ((Number) from.getAsMap().get(toField.getName())).intValue();
        }
        if (toField.getType().equals(Float.class) || toField.getType().equals(float.class)) {
            return ((Number) from.getAsMap().get(toField.getName())).floatValue();
        }
        return from.getAsMap().get(toField.getName());
    }

    public static <T> T populateList(DBObject dbObject) {
        return dbObject.get(DBObject.CHILDREN);
    }
}
