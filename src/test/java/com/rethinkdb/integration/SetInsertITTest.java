package com.rethinkdb.integration;

import com.rethinkdb.model.MapObject;
import com.rethinkdb.response.model.DMLResult;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SetInsertITTest extends AbstractITTest {

    @Test
    public void testSetInsert() {
        DMLResult insertResult = r
                .db(dbName)
                .table(tableName)
                .insert(new MapObject()
                        .with("tags", new ArrayList<Object>()))
                .run(con);
        String id = insertResult.getGenerated_keys().get(0);

        ArrayList<Object> newTags = new ArrayList<Object>();
        newTags.add("newTag");

        DMLResult updateResult = r
                .db(dbName)
                .table(tableName)
                .get(id)
                .update(new MapObject()
                        .with("tags", r.row().field("tags").setInsert(newTags)))
                .run(con);

        Assertions.assertThat((List)r.db(dbName).table(tableName).run(con).get(0).get("tags")).contains("newTag");
    }
}
