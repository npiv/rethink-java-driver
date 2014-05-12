package com.rethinkdb.integration;

import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.model.DBObjectBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

public class FilteringIT extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(
                new DBObjectBuilder().with("name", "superman").with("age", 30).build(),
                new DBObjectBuilder().with("name", "spiderman").with("age", 23).build(),
                new DBObjectBuilder().with("name", "heman").with("age", 55).build()
        ).run(con);
    }

    @Test
    public void testGT() {
        Assertions.assertThat(
            r.db(dbName).table(tableName).filter(r.lambda(new DBLambda() {
                @Override
                public RTFluentQuery apply(RTFluentQuery row) {
                    return row.field("age").gt(30);
                }
            })).run(con).size()
        ).isEqualTo(1);
    }

    @Test
    public void testGTandLT() {
        Assertions.assertThat(
                r.db(dbName).table(tableName).filter(r.lambda(new DBLambda() {
                    @Override
                    public RTFluentQuery apply(RTFluentQuery row) {
                        return r.or(
                                r.and(
                                        row.field("age").gt(20),
                                        row.field("age").lt(30)
                                ),
                                row .field("name").eq("heman")
                        );
                    }
                })).run(con).size()
        ).isEqualTo(2);
    }
}
