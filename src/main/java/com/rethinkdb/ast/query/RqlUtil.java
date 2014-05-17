package com.rethinkdb.ast.query;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.ast.query.gen.Datum;
import com.rethinkdb.ast.query.gen.Func;
import com.rethinkdb.ast.query.gen.MakeArray;
import com.rethinkdb.ast.query.gen.MakeObj;
import com.rethinkdb.proto.Q2L;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.*;
import java.util.*;
import java.util.Date;
import java.util.Map;

public class RqlUtil {
    /**
     * Coerces objects from their native type to RqlQuery
     * @param val val
     * @return RqlQuery
     */
    public static RqlQuery toRqlQuery(java.lang.Object val) {
        return toRqlQuery(val, 20);
    }

    private static RqlQuery toRqlQuery(java.lang.Object val, int remainingDepth) {
        if (val instanceof RqlQuery) {
            return (RqlQuery)val;
        }

        if (val instanceof List) {
            List<java.lang.Object> innerValues = new ArrayList<java.lang.Object>();
            for (java.lang.Object innerValue : (List) val) {
                innerValues.add(toRqlQuery(innerValue, remainingDepth - 1));
            }
            return new MakeArray(innerValues, null);
        }

        if (val instanceof Map) {
            Map<String, Object> obj = new HashMap<String, Object>();
            for (Map.Entry<Object, Object> entry : (Set<Map.Entry>) ((Map) val).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new RethinkDBException("Object key can only be strings");
                }

                obj.put((String)entry.getKey(), toRqlQuery(entry.getValue()));
            }
            return new MakeObj(obj);
        }

        if (val instanceof RqlFunction) {
            return new Func((RqlFunction) val);
        }

        if (val instanceof Date) {
            throw new NotImplementedException();
        }

        return new Datum(val);
    }


    /*
        Called on arguments that should be functions
     */
    public static RqlQuery funcWrap(java.lang.Object o) {
        final RqlQuery rqlQuery = toRqlQuery(o);

        if (hasImplicitVar(rqlQuery)) {
            return new Func(new RqlFunction() {
                @Override
                public RqlQuery apply(RqlQuery row) {
                    return rqlQuery;
                }
            });
        }

        else {
            return rqlQuery;
        }
    }


    public static boolean hasImplicitVar(RqlQuery node) {
        if (node.getTermType() == Q2L.Term.TermType.IMPLICIT_VAR) {
            return true;
        }
        for (RqlQuery arg : node.getArgs()) {
            if (hasImplicitVar(arg)) {
                return true;
            }
        }
        for (Map.Entry<String, RqlQuery> kv : node.getOptionalArgs().entrySet()) {
            if (hasImplicitVar(kv.getValue())) {
                return true;
            }
        }

        return false;
    }
    public static Q2L.Datum createDatum(java.lang.Object value) {
        Q2L.Datum.Builder builder = Q2L.Datum.newBuilder();

        if (value == null) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_NULL)
                    .build();
        }

        if (value instanceof String) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_STR)
                    .setRStr((String) value)
                    .build();
        }

        if (value instanceof Number) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_NUM)
                    .setRNum(((Number) value).doubleValue())
                    .build();
        }

        if (value instanceof Boolean) {
            return builder
                    .setType(Q2L.Datum.DatumType.R_BOOL)
                    .setRBool((Boolean) value)
                    .build();
        }

        if (value instanceof Collection) {
            Q2L.Datum.Builder arr = builder
                    .setType(Q2L.Datum.DatumType.R_ARRAY);

            for (java.lang.Object o : (Collection) value) {
                arr.addRArray(createDatum(o));
            }

            return arr.build();

        }

        throw new RethinkDBException("Unknown Value can't create datatype for : " + value.getClass());
    }

}
