package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.ast.MapObject;
import com.rethinkdb.ast.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class LambdasITTest extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(Lists.<Map<String, Object>>newArrayList(
            new MapObject().with("name", "superman").with("age", 30),
            new MapObject().with("name", "spiderman").with("age", 23),
            new MapObject().with("name", "heman").with("age", 55))
        ).run(con);
    }

    @Test
    public void testMap() {
        List<Double> ages = r.db(dbName).table(tableName).map(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("age").add(20);
            }
        }).run(con());

        Assertions.assertThat(ages).contains(50.0, 43.0, 75.0);
    }

}
