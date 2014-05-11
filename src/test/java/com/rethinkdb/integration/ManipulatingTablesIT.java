package com.rethinkdb.integration;

import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.response.InsertResult;
import org.fest.assertions.Assertions;
import org.junit.Test;

public class ManipulatingTablesIT extends AbstractITTest {

    @Test
    public void createListAndDropTable() {
        Assertions.assertThat(r.db(dbName).tableList().run(con)).contains(tableName);
        r.db(dbName).tableDrop(tableName).run(con);
        Assertions.assertThat(r.db(dbName).tableList().run(con)).excludes(tableName);
        r.db(dbName).tableCreate(tableName).run(con);
        Assertions.assertThat(r.db(dbName).tableList().run(con)).contains(tableName);
    }

    @Test
    public void createTableWithPrimaryKey() {
        String customPKTable = "customPKTable";

        r.db(dbName).tableCreate(customPKTable, "myId", null, null).run(con);
        InsertResult res1 = r.db(dbName).table(customPKTable).insert(new DBObjectBuilder().with("myId", 1).build()).run(con);
        InsertResult res2 = r.db(dbName).table(customPKTable).insert(new DBObjectBuilder().with("myId", 1).build()).run(con);

        Assertions.assertThat(res1.getInserted()).isEqualTo(1);
        Assertions.assertThat(res2.getInserted()).isEqualTo(0); // duplicate id
    }


}
