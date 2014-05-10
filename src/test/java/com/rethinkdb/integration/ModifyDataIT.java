package com.rethinkdb.integration;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ModifyDataIT extends AbstractITTest {

    @Test
    @Ignore
    public void testModify() {
        List<DBObject> objects = new ArrayList<DBObject>();
        for (int i = 0; i < 10; i++) {
            objects.add(new DBObjectBuilder().with("abc", i).build());
        }
        r.db(dbName).table(tableName).insert(objects).run(con);

        DBObject result = r.db(dbName).table(tableName).run(con);
        // TODO make a DBObjectList
        Assertions.assertThat(((List)result.get(DBObject.CHILDREN)).get(0)).isEqualTo(0);
    }

}
