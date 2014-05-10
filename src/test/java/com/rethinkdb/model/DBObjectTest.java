package com.rethinkdb.model;


import org.fest.assertions.Assertions;
import org.junit.Test;

import java.util.HashMap;

public class DBObjectTest {

    @Test
    public void testBuildFromMap() {
        DBObject dbObject = DBObjectBuilder.buildFromMap(new HashMap<String, Object>() {{
            put("aString", "test");
            put("aNumber", 1234);
        }});

        Assertions.assertThat(dbObject.get("aString")).isEqualTo("test");
        Assertions.assertThat(dbObject.get("aNumber")).isEqualTo(1234);
    }

    @Test
    public void testBuildManual() {
        DBObject dbObject = new DBObjectBuilder()
                .with("aString", "test")
                .with("aNumber", 1234)
                .build();

        Assertions.assertThat(dbObject.get("aString")).isEqualTo("test");
        Assertions.assertThat(dbObject.get("aNumber")).isEqualTo(1234);
    }

}
