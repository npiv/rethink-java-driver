package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class FilteringIT extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(Lists.<Map<String,Object>>newArrayList(
                new MapObject().with("name", "superman").with("age", 30),
                new MapObject().with("name", "spiderman").with("age", 23),
                new MapObject().with("name", "heman").with("age", 55)
        )).run(con);
    }

    @Test
    public void testGT() {
        Assertions.assertThat(
                r.db(dbName).table(tableName).filter(new RqlFunction() {
                     @Override
                     public RqlQuery apply(RqlQuery row) {
                         return row.field("age").gt(30);
                     }
                 }
                ).run(con())
        ).hasSize(1);
    }

    @Test
    public void testGTandLT() {
        Assertions.assertThat(
                r.db(dbName).table(tableName).filter(new RqlFunction() {

                    @Override
                    public RqlQuery apply(RqlQuery row) {
                        return r.or(
                                r.and(
                                        row.field("age").gt(20),
                                        row.field("age").lt(30)
                                ),
                                row .field("name").eq("heman")
                        );
                    }
                }
                ).run(con).size()
        ).isEqualTo(2);
    }
}
