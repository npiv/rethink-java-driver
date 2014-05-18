package com.rethinkdb.integration;

import com.google.common.collect.Lists;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.model.RqlFunction2;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class GroupIT extends AbstractITTest {

    @Before
    public void setUpSimpleTable() {
        r.db(dbName).table(tableName).insert(
                new MapObject().with("id", 2).with("player", "Bob").with("points", 15).with("type", "ranked"),
                new MapObject().with("id", 5).with("player", "Alice").with("points", 7).with("type", "free"),
                new MapObject().with("id", 11).with("player", "Bob").with("points", 10).with("type", "free"),
                new MapObject().with("id", 12).with("player", "Alice").with("points", 2).with("type", "free")
        ).run(con);
    }

   @Test
    public void testDocsGroup() {
       Assertions.assertThat(
               r.table(tableName).map(new RqlFunction() {
                   @Override
                   public RqlQuery apply(RqlQuery row) {
                       return r.expr(1);
                   }
               }).reduce(new RqlFunction2() {
                   @Override
                   public RqlQuery apply(RqlQuery row1, RqlQuery row2) {
                       return row1.add(row2);
                   }
               }).run(con)
       ).isEqualTo(4.0);
   }

    @Test
    public void testSum() {
        Assertions.assertThat(r.expr(Lists.newArrayList(3, 5, 7)).sum().run(con)).isEqualTo(15.0);
    }

    @Test
    public void testHasFields() {
        Assertions.assertThat(r.table(tableName).hasFields(Lists.newArrayList("points")).run(con)).hasSize(4);
    }

    @Test
    public void testInsertAt() {
        Assertions.assertThat(r.expr(Lists.newArrayList("a","b")).insertAt(1,"c").run(con)).hasSize(3);
    }
}
