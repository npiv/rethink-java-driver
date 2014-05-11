package com.rethinkdb.integration;

import com.rethinkdb.model.DBObjectBuilder;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.List;

public class FetchDataIT extends AbstractITTest {

    @Test
    public void testGetByPKString() {
        Assertions.assertThat( r.table(tableName).get("test").run(con) ).isNull();

        r.db(dbName).table(tableName).insert( new DBObjectBuilder().with("id", "test").build() ).run(con);

        Assertions.assertThat( r.table(tableName).get("test").run(con) ).isNotNull();
    }

    @Test
    public void testGetByPKNumber() {
        Assertions.assertThat( r.table(tableName).get(1).run(con) ).isNull();

        r.db(dbName).table(tableName).insert( new DBObjectBuilder().with("id", 1).build() ).run(con);

        Assertions.assertThat( r.table(tableName).get(1).run(con) ).isNotNull();
    }

    @Test
    public void testGetAll() {
        Assertions.assertThat( r.table(tableName).getAll(1, 2).run(con) ).isEmpty();

        r.db(dbName).table(tableName).insert( new DBObjectBuilder().with("id", 1).build() ).run(con);

        Assertions.assertThat( r.table(tableName).getAll(1, 2).run(con) ).hasSize(1);

        r.db(dbName).table(tableName).insert( new DBObjectBuilder().with("id", 2).build() ).run(con);

        Assertions.assertThat( r.table(tableName).getAll(1, 2).run(con) ).hasSize(2);
    }

    @Test
    public void testGetAllCustomIndex() {
        // TODO add this test when we have ablitity to add indexes to tables
    }
}
