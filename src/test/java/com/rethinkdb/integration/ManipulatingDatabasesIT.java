package com.rethinkdb.integration;

import org.fest.assertions.Assertions;
import org.junit.Test;

public class ManipulatingDatabasesIT extends AbstractITT2est {

    @Test
    public void createListAndDropDatabase() {
        Assertions.assertThat(r.dbList().run(con)).contains(dbName);
        r.dbDrop(dbName).run(con);
        Assertions.assertThat(r.dbList().run(con)).excludes(dbName);
        r.dbCreate(dbName).run(con);
        Assertions.assertThat(r.dbList().run(con)).contains(dbName);
    }
}
