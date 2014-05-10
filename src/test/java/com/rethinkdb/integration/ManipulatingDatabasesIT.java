package com.rethinkdb.integration;

import com.rethinkdb.response.StringListDBResult;
import org.fest.assertions.Assertions;
import org.junit.Test;

public class ManipulatingDatabasesIT extends AbstractITTest {

    @Test
    public void createListAndDropDatabase() {
        Assertions.assertThat(  r.dbList().run(con).getResult()  ).contains(dbName);
        r.dbDrop(dbName).run(con);
        Assertions.assertThat(  r.dbList().run(con).getResult()  ).excludes(dbName);
        r.dbCreate(dbName).run(con);
        Assertions.assertThat(  r.dbList().run(con).getResult()  ).contains(dbName);
    }
}
