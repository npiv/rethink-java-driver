package com.rethinkdb.integration;

import com.rethinkdb.fluent.RTFluentRow;
import com.rethinkdb.model.DBLambda;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.fluent.option.Durability;
import com.rethinkdb.response.model.DMLResult;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ModifyDataIT extends AbstractITTest {

    @Test
    public void testInsertList() {
        List<DBObject> objects = new ArrayList<DBObject>();
        for (int i = 0; i < 10; i++) {
            objects.add(new DBObjectBuilder().with("abc", i).build());
        }
        r.db(dbName).table(tableName).insert(objects).run(con);

        List<DBObject> result = r.db(dbName).table(tableName).run(con);
        List<Double> setKeys = new ArrayList<Double>();
        for (DBObject dbObject : result) {
            setKeys.add((Double)dbObject.get("abc"));
        }

        Assertions.assertThat(setKeys).contains(0.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0);
    }

    @Test
      public void doubleInsertFailsWithoutUpsert() {
        DMLResult firstResult = r.db(dbName).table(tableName)
                .insert(new DBObjectBuilder().with("id", 1).build()).run(con);
        DMLResult secondResult = r.db(dbName).table(tableName)
                .insert(new DBObjectBuilder().with("id", 1).with("field", "a").build()).run(con);

        Assertions.assertThat(firstResult.getInserted()).isEqualTo(1);

        Assertions.assertThat(secondResult.getReplaced()).isEqualTo(0);
        Assertions.assertThat(secondResult.getInserted()).isEqualTo(0);
    }

    @Test
    public void doubleInsertWorksWithUpsert() {
        DMLResult firstResult = r.db(dbName).table(tableName)
                .insert(new DBObjectBuilder().with("id", 1).build(), Durability.hard, false, true).run(con);
        DMLResult secondResult = r.db(dbName).table(tableName)
                .insert(new DBObjectBuilder().with("id", 1).with("field", "a").build(), Durability.hard, false, true).run(con);

        Assertions.assertThat(firstResult.getInserted()).isEqualTo(1);

        Assertions.assertThat(secondResult.getReplaced()).isEqualTo(1);
        Assertions.assertThat(secondResult.getInserted()).isEqualTo(0);
    }


    @Test
    public void updateFromTable() {
        r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("id", 1).build()).run(con);
        r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("id", 2).build()).run(con);

        DMLResult result = r.db(dbName).table(tableName).update(new DBObjectBuilder().with("name", "test").build()).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(2);
    }

    @Test
    public void updateFromGet() {
        r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("id", 1).build()).run(con);

        DMLResult result = r.db(dbName).table(tableName).get(1).update(new DBObjectBuilder().with("name", "test").build()).run(con);

        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }

    @Test
    public void testReplace() {
        DBObject replacement = new DBObjectBuilder().with("id", 1).with("field1", "abc").build();

        DMLResult result = r.db(dbName).table(tableName).replace(replacement).run(con);
        Assertions.assertThat(result.getReplaced()).isEqualTo(0);

        r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("id", 1).build()).run(con);

        result = r.db(dbName).table(tableName).replace(replacement).run(con);
        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }

    @Test
    public void testReplace_Without() {
        DBObject dbObj = new DBObjectBuilder().with("id", 1).with("field1", "abc").build();
        r.db(dbName).table(tableName).insert(dbObj).run(con);


        DMLResult result = r.db(dbName).table(tableName).replace(new DBLambda() {
            @Override
            public RTFluentRow apply(RTFluentRow row) {
                return row.without("field1");
            }
        }).run(con);

        System.out.println(result);
        Assertions.assertThat(result.getReplaced()).isEqualTo(1);
    }

    @Test
    public void testWithout() {
        DBObject dbObj = new DBObjectBuilder().with("id", 1).with("field1", "abc").build();
        r.db(dbName).table(tableName).insert(dbObj).run(con);

        List<DBObject> objects = r.db(dbName).table(tableName).without("field1").run(con);

        Assertions.assertThat(objects.get(0).get("field1")).isNull();
        Assertions.assertThat(objects.get(0).get("id")).isEqualTo(1.0);

    }

}
