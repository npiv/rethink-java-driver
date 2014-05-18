package com.rethinkdb.integration;

import com.rethinkdb.model.MapObject;
import com.rethinkdb.response.model.DMLResult;
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
        DMLResult res1 = r.db(dbName).table(customPKTable).insert(new MapObject().with("myId", 1)).run(con);
        DMLResult res2 = r.db(dbName).table(customPKTable).insert(new MapObject().with("myId", 1)).run(con);

        Assertions.assertThat(res1.getInserted()).isEqualTo(1);
        Assertions.assertThat(res2.getInserted()).isEqualTo(0); // duplicate id
    }

    @Test
    public void testIndexList() {
        Assertions.assertThat(r.db(dbName).table(tableName).indexCreate("wee").run(con).getCreated()).isEqualTo(1);
        Assertions.assertThat(r.db(dbName).table(tableName).indexList().run(con())).containsExactly("wee");
        Assertions.assertThat(r.db(dbName).table(tableName).indexDrop("wee").run(con).getDropped()).isEqualTo(1);
        Assertions.assertThat(r.db(dbName).table(tableName).indexList().run(con())).isEmpty();
    }

    @Test
    public void testIndexStatus() {
        Assertions.assertThat(r.db(dbName).table(tableName).indexCreate("wee").run(con).getCreated()).isEqualTo(1);
        Assertions.assertThat(r.db(dbName).table(tableName).indexStatus().run(con()).get(0).isReady()).isTrue();

        System.out.println(r.db(dbName).table(tableName).indexWait().run(con()));
    }


}
