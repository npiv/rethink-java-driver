package com.rethinkdb.integration;

import com.rethinkdb.model.ConflictStrategy;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.Durability;
import com.rethinkdb.response.model.DMLResult;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ModifyDataIT extends AbstractITTest {

    @Test
    public void testInsertList() {
        List<Map<String, Object>> objects = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 10; i++) {
            objects.add(new MapObject().with("abc", i));
        }
        r.db(dbName).table(tableName).insert(objects).run(con);

        // TODO runForType here
        List<Map<String, Object>> result = (List<Map<String, Object>>) r.db(dbName).table(tableName).run(con);

        List<Double> setKeys = new ArrayList<Double>();
        for (Map<String, Object> map : result) {
            setKeys.add((Double) map.get("abc"));
        }

        Assertions.assertThat(setKeys).contains(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0);
    }

    @Test
    public void doubleInsertFailsWithoutUpsert() {
        DMLResult firstResult = r.db(dbName).table(tableName)
                .insert(new MapObject().with("id", 1)).run(con);
        DMLResult secondResult = r.db(dbName).table(tableName)
                .insert(new MapObject().with("id", 1).with("field", "a")).run(con);

        Assertions.assertThat(firstResult.getInserted()).isEqualTo(1);

        Assertions.assertThat(secondResult.getReplaced()).isEqualTo(0);
        Assertions.assertThat(secondResult.getInserted()).isEqualTo(0);
    }

    @Test
    public void doubleInsertWorksWithUpsert() {
        DMLResult firstResult = r.db(dbName).table(tableName)
                .insert(new MapObject().with("id", 1), Durability.hard, false, ConflictStrategy.replace).run(con);
        DMLResult secondResult = r.db(dbName).table(tableName)
                .insert(new MapObject().with("id", 1).with("field", "a"), Durability.hard, false, ConflictStrategy.replace).run(con);

        Assertions.assertThat(firstResult.getInserted()).isEqualTo(1);

        Assertions.assertThat(secondResult.getReplaced()).isEqualTo(1);
        Assertions.assertThat(secondResult.getInserted()).isEqualTo(0);
    }


    @Test
    public void updateFromTable() {
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 1)).run(con);
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 2)).run(con);

        DMLResult result = r.db(dbName).table(tableName).update(new MapObject().with("name", "test")).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(2);
    }

    @Test
    public void updateFromGet() {
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 1)).run(con);

        DMLResult result = r.db(dbName).table(tableName).get(1).update(new MapObject().with("name", "test")).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }


    @Test
    public void updateWithLambdaInObject() {
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 1).with("age", 1)).run(con);
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 2).with("age", 2)).run(con);

        DMLResult result = RqlQuery.R.db(dbName).table(tableName)
                .update(new MapObject().with("age", r.row().field("age").add(22))).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(2);
    }

    @Test
    public void updateBranch() {
        r.db(dbName).table(tableName).insert(new MapObject().with("id", 1).with("age", 1)).run(con);

        DMLResult result = RqlQuery.R.db(dbName).table(tableName)
                .update(
                        new RqlFunction() {
                            @Override
                            public RqlQuery apply(RqlQuery row) {
                                return r.branch(
                                        row.field("age").gt(0),
                                        new MapObject().with("born", true),
                                        new MapObject().with("born", false)
                                );
                            }
                        }
                ).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }


    @Test
    public void testReplace() {
        Map<String, Object> replacement = new MapObject().with("id", 1).with("field1", "abc");

        DMLResult result = r.db(dbName).table(tableName).replace(replacement).run(con);
        Assertions.assertThat(result.getReplaced()).isEqualTo(0);

        r.db(dbName).table(tableName).insert(new MapObject().with("id", 1)).run(con);

        result = r.db(dbName).table(tableName).replace(replacement).run(con);
        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }

    @Test
    public void testReplace_Without() { // as lambda
        Map<String, Object> dbObj = new MapObject().with("id", 1).with("field1", "abc");
        r.db(dbName).table(tableName).insert(dbObj).run(con);

        DMLResult result = r.db(dbName).table(tableName).replace(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.without("field1");
            }
        }).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }

    @Test
    public void testReplace_pluck() { // as lambda
        Map<String, Object> dbObj = new MapObject()
                .with("id", 1)
                .with("field1", "abc")
                .with("field2", "def");

        r.db(dbName).table(tableName).insert(dbObj).run(con);

        List<Map<String, Object>> result = (List<Map<String, Object>>) r.db(dbName).table(tableName).pluck(Arrays.asList("field1", "field2")).run(con);
        Assertions.assertThat(result.get(0).get("field1")).isEqualTo("abc");
        Assertions.assertThat(result.get(0).get("field2")).isEqualTo("def");

        result = (List<Map<String, Object>>) r.db(dbName).table(tableName).pluck(Arrays.asList("field2")).run(con);
        Assertions.assertThat(result.get(0).get("field1")).isNull();
        Assertions.assertThat(result.get(0).get("field2")).isEqualTo("def");

    }

    @Test
    public void testWithout() { // as normal function
        Map<String, Object> dbObj = new MapObject().with("id", 1).with("field1", "abc");
        r.db(dbName).table(tableName).insert(dbObj).run(con);

        List<Map<String, Object>> objects = (List<Map<String, Object>>) r.db(dbName).table(tableName).without("field1").run(con);

        Assertions.assertThat(objects.get(0).get("field1")).isNull();
        Assertions.assertThat(objects.get(0).get("id")).isEqualTo(1.0);
    }

    @Test
    public void testCountAndDelete() {
        Map<String, Object> dbObj = new MapObject().with("id", 1);
        r.db(dbName).table(tableName).insert(dbObj).run(con);

        Assertions.assertThat(r.table(tableName).count().run(con)).isEqualTo(1);
        r.db(dbName).table(tableName).delete().run(con);
        Assertions.assertThat(r.table(tableName).count().run(con)).isEqualTo(0);

    }

    @Test
    public void testSync() {
        r.table(tableName).sync(); // no crash
    }



}
