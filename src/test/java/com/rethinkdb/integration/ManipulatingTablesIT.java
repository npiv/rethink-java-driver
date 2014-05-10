package com.rethinkdb.integration;

import com.rethinkdb.RethinkDBException;
import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.query.option.Durability;
import com.rethinkdb.response.InsertResult;
import com.rethinkdb.response.StringListDBResult;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Test;

public class ManipulatingTablesIT extends AbstractITTest {

    private static final String tableName = "test_table_123";

    @Test
    public void createListAndDropTable() {
        Assertions.assertThat(r.db(dbName).tableList().run(con).getResult()).excludes(tableName);
        r.db(dbName).tableCreate(tableName).run(con);
        Assertions.assertThat(r.db(dbName).tableList().run(con).getResult()).contains(tableName);
        r.db(dbName).tableDrop(tableName).run(con);
        Assertions.assertThat(r.db(dbName).tableList().run(con).getResult()).excludes(tableName);
    }

    @Test
    public void createTableWithPrimaryKey() {
        r.db(dbName).tableCreate(tableName,"myId", null, null).run(con);
        InsertResult res1 = r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("myId", 1).build()).run(con);
        InsertResult res2 = r.db(dbName).table(tableName).insert(new DBObjectBuilder().with("myId", 1).build()).run(con);

        Assertions.assertThat(res1.getInserted()).isEqualTo(1);
        Assertions.assertThat(res2.getInserted()).isEqualTo(0); // duplicate id
    }


}
