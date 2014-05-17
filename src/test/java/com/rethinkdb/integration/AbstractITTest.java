package com.rethinkdb.integration;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.RethinkDBException;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.Durability;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Properties;

public abstract class AbstractITTest {

    protected RqlQuery r = RqlQuery.R;
    
    protected static RethinkDBConnection con;

    protected static final String dbName = "test_db_123";
    protected static final String tableName = "test_table_123";

    @Before
    public void createDb() {
        try {
            r.dbCreate(dbName).run(con);
            con.use(dbName);
            r.db(dbName).tableCreate(tableName,null, Durability.hard, null).run(con);
        } catch (RethinkDBException ex) {
            ex.printStackTrace();
        }
    }

    @After
    public void dropDb() {
        try {
            r.dbDrop(dbName).run(con);
        } catch (RethinkDBException ex) {
            ex.printStackTrace();
        }
    }

    @BeforeClass
    public static void initCon() {
        con = con();
    }

    @AfterClass
    public static void tearDownCon() {
        con.close();
    }

    public static RethinkDBConnection con() {
        try {
            Properties props = new Properties();
            props.load(AbstractITTest.class.getResourceAsStream("/dbtest.properties"));
            return RethinkDB.r.connect(props.getProperty("hostname"));
        } catch (IOException e) {
            throw new RuntimeException("Couldn't load db properties", e);
        }
    }
}
