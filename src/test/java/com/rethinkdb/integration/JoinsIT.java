package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.model.RqlFunction2;
import com.rethinkdb.response.model.JoinResult;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class JoinsIT extends AbstractITTest {

    private String tableChildren = "femaleSuperHeros";

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(Lists.<Map<String, Object>>newArrayList(
                new MapObject().with("name", "Superman").with("age", 30),
                new MapObject().with("name", "Spiderman").with("age", 23),
                new MapObject().with("name", "Heman").with("age", 55)
        )).run(con);

        r.db(dbName).tableCreate(tableChildren).run(con());

        r.db(dbName).table(tableChildren).insert(Lists.<Map<String, Object>>newArrayList(
                new MapObject().with("name", "Spidergirl").with("age", 23),
                new MapObject().with("name", "Xena").with("age", 55)
        )).run(con);
    }

    @Test
    public void testInnerJoin() {
        List<JoinResult> result = r.db(dbName).table(tableName).innerJoin(
                r.db(dbName).table(tableChildren),
                new RqlFunction2() {
                    @Override
                    public RqlQuery apply(RqlQuery row1, RqlQuery row2) {
                        return row1.field("age").eq(row2.field("age"));
                    }
                }
        ).run(con);

        Assertions.assertThat(result).hasSize(2);

        for (JoinResult joinResult : result) {
            Assertions.assertThat(joinResult.getLeft().get("age")).isEqualTo(joinResult.getRight().get("age"));
        }
    }

    @Test
    public void testOuterJoin() {
        List<JoinResult> result = r.db(dbName).table(tableName).outerJoin(
                r.db(dbName).table(tableChildren),
                new RqlFunction2() {
                    @Override
                    public RqlQuery apply(RqlQuery row1, RqlQuery row2) {
                        return row1.field("age").eq(row2.field("age"));
                    }
                }
        ).run(con);

        Assertions.assertThat(result).hasSize(3);
    }

    @Test
    public void testZip() {
        List<Map<String,Object>> result = r.db(dbName).table(tableName).outerJoin(
                r.db(dbName).table(tableChildren),
                new RqlFunction2() {
                    @Override
                    public RqlQuery apply(RqlQuery row1, RqlQuery row2) {
                        return row1.field("age").eq(row2.field("age"));
                    }
                }
        ).zip().run(con);

        Assertions.assertThat(result).hasSize(3);
    }

}
