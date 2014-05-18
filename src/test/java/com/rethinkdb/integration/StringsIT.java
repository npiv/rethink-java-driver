package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class StringsIT extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(Lists.<Map<String, Object>>newArrayList(
                new MapObject().with("name", "Superman").with("age", 30),
                new MapObject().with("name", "Spiderman").with("age", 23),
                new MapObject().with("name", "Heman").with("age", 55)
        )).run(con);
    }

    @Test
    public void testUpcase() {
         List<String> strings = r.db(dbName).table(tableName).map(new RqlFunction() {
             @Override
             public RqlQuery apply(RqlQuery row) {
                 return row.field("name").upcase();
             }
         }).runTyped(con);

        Assertions.assertThat(strings).contains("SUPERMAN", "SPIDERMAN", "HEMAN");
    }

    @Test
    public void testDowncase() {
        List<String> strings = r.db(dbName).table(tableName).map(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("name").downcase();
            }
        }).runTyped(con);

        Assertions.assertThat(strings).contains("superman", "spiderman", "heman");
    }

    @Test
    public void testSplit() {
        String splitTable = "splitTable";
        r.db(dbName).tableCreate(splitTable).run(con);
        r.db(dbName).table(splitTable).insert(new MapObject().with("name", "aaa bbb")).run(con);

        List<List<String>> strings = r.db(dbName).table(splitTable).map(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("name").split(" ");
            }
        }).runTyped(con());

        Assertions.assertThat(strings.get(0).get(0)).isEqualToIgnoringCase("aaa");
    }

    @Test
    public void testMatch() {
        Map<String, Object> dbObj = new MapObject().with("id", 1).with("field1", "abc");
        r.db(dbName).table(tableName).insert(dbObj).run(con);

        List list = r.db(dbName).table(tableName).filter(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("field1").match("a.c");
            }
        }).run(con());

        Assertions.assertThat(list).hasSize(1);
    }
}
