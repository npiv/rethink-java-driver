package com.rethinkdb.mapper;

import com.rethinkdb.model.DBObject;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RAssocPairBuilder;
import com.rethinkdb.test.PopulateTestObject;
import org.fest.assertions.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class DBObjectMapperTest {

    @Test
    public void testPopulateObject() {
        PopulateTestObject testObject = new PopulateTestObject();

        DBObjectMapper.populateObject(testObject,
                new DBObjectBuilder()
                        .with("anInt", new Float(1.2))
                        .with("aDouble", new Integer(2))
                        .with("aFloat", new Integer(1))
                        .with("aStringList", new ArrayList<String>() {{
                            add("s1");
                            add("s2");
                        }})
                        .with("aString", "string")
                        .with("aNull", (String) null)
                        .build()
        );

        Assertions.assertThat(testObject.getAnInt()).isEqualTo(1);
        Assertions.assertThat(testObject.getaDouble()).isEqualTo(2.0);
        Assertions.assertThat(testObject.getaFloat()).isEqualTo(1);
        Assertions.assertThat(testObject.getaString()).isEqualTo("string");
        Assertions.assertThat(testObject.getaStringList()).containsExactly("s1", "s2");
        Assertions.assertThat(testObject.getaNull()).isNull();

    }


    @Test
    public void testUnmappedFieldsTolerated() {
        PopulateTestObject testObject = new PopulateTestObject();

        DBObjectMapper.populateObject(testObject, new DBObjectBuilder().build());

        Assertions.assertThat(testObject.getAnInt()).isEqualTo(0);
        Assertions.assertThat(testObject.getaDouble()).isEqualTo(0.0);
        Assertions.assertThat(testObject.getaFloat()).isEqualTo(0);
        Assertions.assertThat(testObject.getaString()).isEqualTo(null);
        Assertions.assertThat(testObject.getaStringList()).isNull();
        Assertions.assertThat(testObject.getaNull()).isNull();

    }

    @Test
    public void testToDBObjectFromDatum() {
        Q2L.Datum datum = Q2L.Datum.newBuilder()
                .setType(Q2L.Datum.DatumType.R_OBJECT)
                .addRObject(RAssocPairBuilder.datumPair("test", 1234.0))
                .build();

        DBObject object = DBObjectMapper.fromDatumObject(datum);

        Assertions.assertThat(object.get("test")).isEqualTo(1234.0);
    }

    @Test
    public void testErrorForAnyTypeBesideObject() {
        for (Q2L.Datum.DatumType dt : Q2L.Datum.DatumType.values()) {
            if (dt != Q2L.Datum.DatumType.R_OBJECT) {
                Q2L.Datum datum = Q2L.Datum.newBuilder()
                        .setType(Q2L.Datum.DatumType.R_NUM)
                        .setRNum(22.0)
                        .build();

                try {
                    DBObjectMapper.fromDatumObject(datum);
                    Assert.fail("shouldn't work for " + dt);
                } catch (Exception ex) {
                }
            }
        }
    }
}
