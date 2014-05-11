package com.rethinkdb.proto;

import java.util.List;

/**
 * Utilities to make traversing ProtoBuf objects easier
 */
public class ProtoUtil {
    private ProtoUtil() {
    }

    /**
     * Checks is a list of Protobuf AssocPairs contains a certain key
     *
     * @param pairs pairs to check
     * @param key   key to find
     * @return if key was present
     */
    public static boolean hasKey(List<Q2L.Query.AssocPair> pairs, String key) {
        for (Q2L.Query.AssocPair pair : pairs) {
            if (pair.getKey().equals(key)) return true;
        }
        return false;
    }
}
