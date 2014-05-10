package com.rethinkdb.proto;

import java.util.List;

public class ProtoUtil {
    private ProtoUtil() {}

    public static boolean hasKey(List<Q2L.Query.AssocPair> pairs, String key) {
        for (Q2L.Query.AssocPair pair : pairs) {
            if (pair.getKey().equals(key)) return true;
        }
        return false;
    }
}
