package com.rethinkdb.integration;

import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class StringsIT extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(
                new DBObjectBuilder().with("name", "Superman").with("age", 30).build(),
                new DBObjectBuilder().with("name", "Spiderman").with("age", 23).build(),
                new DBObjectBuilder().with("name", "Heman").with("age", 55).build()
        ).run(con);
    }

    @Test
    public void testUpcase() {
         List<String> strings = r.db(dbName).table(tableName).mapToString(r.lambda(new DBLambda() {
                    @Override
                    public RTFluentQuery apply(RTFluentQuery row) {
                        return row.field("name").upcase();
                    }
                })).run(con);

        Assertions.assertThat(strings).contains("SUPERMAN", "SPIDERMAN", "HEMAN");
    }

    @Test
    public void testDowncase() {
        List<String> strings = r.db(dbName).table(tableName).mapToString(r.lambda(new DBLambda() {
            @Override
            public RTFluentQuery apply(RTFluentQuery row) {
                return row.field("name").downcase();
            }
        })).run(con);

        Assertions.assertThat(strings).contains("superman", "spiderman", "heman");
    }

    @Test
    public void testSplit() {
        String splitTable = "splitTable";
        r.db(dbName).tableCreate(splitTable).run(con);
        r.db(dbName).table(splitTable).insert(new DBObjectBuilder().with("name", "aaa bbb").build()).run(con);

        List<List<String>> strings = r.db(dbName).table(splitTable).map(r.lambda(new DBLambda() {
            @Override
            public RTFluentQuery apply(RTFluentQuery row) {
                return row.field("name").split(" ");
            }
        })).run(con);

        Assertions.assertThat(strings.get(0).get(0)).isEqualToIgnoringCase("aaa");
    }
}
