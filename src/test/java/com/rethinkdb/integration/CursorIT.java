package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.Cursor;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.ast.query.gen.Table;
import com.rethinkdb.model.ConflictStrategy;
import com.rethinkdb.model.Durability;
import com.rethinkdb.model.MapObject;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CursorIT extends AbstractITTest {

    @Test
    public void testSimpleCursor() {
        r.db(dbName).table(tableName).insert( new MapObject().with("id", 1) ).run(con);

        Assertions.assertThat( r.table(tableName).getAll(Lists.<Object>newArrayList(1,2)).run(con) ).hasSize(1);

        r.db(dbName).table(tableName).insert( new MapObject().with("id", 2) ).run(con);

        Assertions.assertThat( r.table(tableName).getAll(Lists.<Object>newArrayList(1,2)).run(con) ).hasSize(2);
    }


    @Test
    public void testWith1001Entries() throws InterruptedException {

        List<Map<String,Object>> obs = Lists.newArrayList();
        for (int i = 0; i < 1001; i++) {
            final long counted = i;
            obs.add(new HashMap<String, Object>() {{ put("counter", counted);  }});
        }
        r.db(dbName).table(tableName).insert(obs, Durability.soft, false, ConflictStrategy.error).run(con);

        Assertions.assertThat( r.db(dbName).table(tableName).run(con) ).hasSize(1001);
    }

    @Test
    public void testNonChangeFeedCursor(){
        Cursor<MapObject> cursor = r.db(dbName).table(tableName).runForCursor(con);
        Assertions.assertThat(cursor.isFeed).isFalse();
    }

    @Test
    public void testChangeFeedCursor() {
        Cursor<MapObject> cursor = r.db(dbName).table(tableName).changes().runForCursor(con);
        Assertions.assertThat(cursor.isFeed).isTrue();
    }

    @Test
    public void testChangeFeed() {

        final String id = "testChangeFeed";
        final MapObject first = new MapObject().with("id", id);
        final MapObject second = new MapObject().with("id", id).with("foo", "bar");
        final Table table = r.db(dbName).table(tableName);

        // Start listening on changefeed
        Cursor<MapObject> cursor = table.changes().runForCursor(con);

        // Make a new connection to create changes
        RethinkDBConnection con2 = con();
        table.insert(first).run(con2);
        table.get(id).update(second).run(con2);
        con2.close();

        // Check out the changefeed
        Assertions.assertThat(cursor.next()).isEqualTo(
                new MapObject()
                        .with("old_val", null)
                        .with("new_val", first)
        );
        Assertions.assertThat(cursor.next()).isEqualTo(
                new MapObject()
                        .with("old_val", first)
                        .with("new_val", second)
        );
    }

    @Test(expected=RethinkDBException.class, timeout=10000)
    public void testChangeFeedTableDeleted() {
        final String id = "testChangeFeedTableDeleted";
        final Table table = r.db(dbName).table(tableName);

        Cursor<MapObject> cursor = table.changes().runForCursor(con);
        Assertions.assertThat(cursor.isClosed()).isFalse();

        // Drop the table we're watching for changes on
        RethinkDBConnection con2 = con();
        r.db(dbName).tableDrop(tableName).run(con2);
        con2.close();

        // trigger an exception
        cursor.next();
    }
}
