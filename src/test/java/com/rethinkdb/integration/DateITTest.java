package com.rethinkdb.integration;

import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.RqlFunction;
import org.fest.assertions.Assertions;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DateITTest extends AbstractITTest {

    private static SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

    @Test
    public void testNow() {
        r.table(tableName).insert(new MapObject().with("date", r.now())).run(con);

        Assertions.assertThat(r.table(tableName).run(con).get(0).get("date")).isInstanceOf(Date.class);
    }

    @Test
    public void testSaveDate() {
        r.table(tableName).insert(new MapObject().with("date", new Date())).run(con);

        Assertions.assertThat(r.table(tableName).run(con).get(0).get("date")).isInstanceOf(Date.class);
    }


    @Test
    public void testTime() {
        r.table(tableName).insert(new MapObject().with("date", r.time(2001, 10, 10))).run(con);

        Assertions.assertThat(sdf.format(r.table(tableName).run(con).get(0).get("date"))).isEqualToIgnoringCase("10-10-2001");
    }

    @Test
    public void testEpochTime() throws ParseException {
        r.table(tableName).insert(new MapObject().with("date", r.epochTime(sdf.parse("10-10-2001").getTime() / 1000))).run(con);

        Assertions.assertThat(sdf.format(r.table(tableName).run(con).get(0).get("date"))).isEqualToIgnoringCase("10-10-2001");
    }

    @Test
    public void testInclusive() throws ParseException {
        r.table(tableName).insert(new MapObject().with("date", r.time(2001, 10, 10))).run(con);
        r.table(tableName).insert(new MapObject().with("date", r.time(2002, 10, 10))).run(con);
        r.table(tableName).insert(new MapObject().with("date", r.time(2003, 10, 10))).run(con);

        List<Map<String, Object>> dates = r.table(tableName).filter(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("date")
                        .during(r.time(2001, 10, 10), r.time(2003, 10, 10), false, false);
            }
        }).run(con);

        Assertions.assertThat(dates).hasSize(1);

        dates = r.table(tableName).filter(new RqlFunction() {
            @Override
            public RqlQuery apply(RqlQuery row) {
                return row.field("date")
                        .during(r.time(2001, 10, 10), r.time(2003, 10, 10), true, true);
            }
        }).run(con);

        Assertions.assertThat(dates).hasSize(3);
    }
}
