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

    @Test
    public void testOrder() {
        List<Double> run = r.db(dbName).table(tableName).orderBy("age").map(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("age");
            }
        }).runTyped(con);

        Assertions.assertThat(run).containsExactly(23.0,30.0,55.0);
    }


    @Test
    public void testOrderDesc() {
        List<Double> run = r.db(dbName).table(tableName).orderBy(r.desc("age"), r.asc("name")).map(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("age");
            }
        }).runTyped(con);

        Assertions.assertThat(run).containsExactly(55.0,30.0,23.0);
    }
}
