package com.rethinkdb.integration;

import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.model.DBObjectBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LambdasITTest extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(
                new DBObjectBuilder().with("name", "superman").with("age", 30).build(),
                new DBObjectBuilder().with("name", "spiderman").with("age", 23).build(),
                new DBObjectBuilder().with("name", "heman").with("age", 55).build()
        ).run(con);
    }

    @Test
    public void testMap() {
        List<Double> ages = r.db(dbName).table(tableName).mapToDouble(r.lambda(new DBLambda() {
            @Override
            public RTFluentQuery apply(RTFluentQuery row) {
                return row.field("age").add(20);
            }
        })).run(con);

        Assertions.assertThat(ages).contains(50.0, 43.0, 75.0);
    }

}
