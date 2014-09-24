package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.model.Durability;
import com.rethinkdb.model.MapObject;
import org.fest.assertions.Assertions;
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
        r.db(dbName).table(tableName).insert(obs, Durability.soft, false, false).run(con);

        Assertions.assertThat( r.db(dbName).table(tableName).run(con) ).hasSize(1001);
    }
}
