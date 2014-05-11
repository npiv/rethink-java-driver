package com.rethinkdb.integration;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.fluent.option.Durability;
import com.rethinkdb.response.DMLResult;
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


}
