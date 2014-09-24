package com.rethinkdb.integration;

import com.rethinkdb.Cursor;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;

import java.util.Map;

public class CursorTest {

    public static RethinkDB r = RethinkDB.r;
    public static void main(String[] args){
        RethinkDBConnection conn = r.connect("newton", 31157);
        conn.use("test");
        Cursor<Map<String, Object>> cursor = r.table("users").changes().runForCursor(conn);
        while(cursor.hasNext()){
            Map<String, Object> val = cursor.next();
            System.out.println(val.get("new_val"));
            System.out.println(val.get("old_val"));
        }
    }
}
