package com.rethinkdb.integration;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import org.fest.assertions.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ModifyDataIT extends AbstractITTest {

    @Test
    public void testModify() {
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

}
