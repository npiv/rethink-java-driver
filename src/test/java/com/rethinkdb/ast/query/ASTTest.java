package com.rethinkdb.ast.query;

import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.integration.AbstractITTest;
import org.junit.Test;

public class ASTTest extends AbstractITTest {

    @Test
    public void testTree() {
        System.out.println(RqlQuery.R.db("test").table("test").get(1).run(con()));
    }
}
