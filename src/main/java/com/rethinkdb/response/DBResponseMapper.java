package com.rethinkdb.response;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.proto.Q2L;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBResponseMapper {

    /**
     * Maps a ProtoBuf Datum object to a java object
     *
     * @param datum datum
     * @return DBObject
     */
    public static <T> T fromDatumObject(Q2L.Datum datum) {
        // Null is Null
        if (datum.getType() == Q2L.Datum.DatumType.R_NULL) {
            return null;
        }

        // Number, Str, bool go to simple java datatypes
        if (datum.getType() == Q2L.Datum.DatumType.R_NUM ||
            datum.getType() == Q2L.Datum.DatumType.R_STR ||
            datum.getType() == Q2L.Datum.DatumType.R_BOOL) {
            return (T)handleType(datum);
        }

        // R_OBJECT is Map
        if (datum.getType() == Q2L.Datum.DatumType.R_OBJECT) {
            Map<String, Object> repr = new HashMap<String, Object>();
            for (Q2L.Datum.AssocPair assocPair : datum.getRObjectList()) {
                repr.put(assocPair.getKey(), handleType(assocPair.getVal()));
            }
            return (T)repr;
        }

        // Array goes to List
        if (datum.getType() == Q2L.Datum.DatumType.R_ARRAY) {
            return (T)makeArray(datum.getRArrayList());
        }

        throw new RethinkDBException("Can't map datum to JavaObject for {}" + datum.getType());

    }

    /**
     * Maps a list of Datum Objects to a list of java objects
     *
     * @param datums datum objects
     * @return DBObject
     */
    public static <T> List<T> fromDatumObjectList(List<Q2L.Datum> datums) {
        List<T> results = new ArrayList<T>();
        for (Q2L.Datum datum : datums) {
            results.add((T)fromDatumObject(datum));
        }
        return results;
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
    public static <T> T populateObject(T to, Map<String,Object> from) {
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

    public static <T> List<T> populateList(Class<T> clazz, List<Map<String, Object>> from) {
        List<T> results = new ArrayList<T>();
        for (Map<String, Object> stringObjectMap : from) {
            try {
                results.add(populateObject(clazz.newInstance(),stringObjectMap));
            } catch (InstantiationException e) {
                throw new RethinkDBException("Error instantiating " + clazz, e);
            } catch (IllegalAccessException e) {
                throw new RethinkDBException("Illegal access on " + clazz, e);
            }
        }
        return results;
    }



    private static Object convertField(Field toField, Map<String, Object> from) {
        if (from.get(toField.getName()) == null) {
            return null;
        }
        if (toField.getType().equals(Integer.class) || toField.getType().equals(int.class)) {
            return ((Number) from.get(toField.getName())).intValue();
        }
        if (toField.getType().equals(Float.class) || toField.getType().equals(float.class)) {
            return ((Number) from.get(toField.getName())).floatValue();
        }
        return from.get(toField.getName());
    }


}
