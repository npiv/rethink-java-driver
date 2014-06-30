## Introduction

[Full Documentation here](http://npiv.github.io/rethink-java-doc/html/)

## Maven Central

Include in your pom with

```xml
<dependency>
    <groupId>com.rethinkdb</groupId>
    <artifactId>rethink-java-driver</artifactId>
    <version>0.3</version>
</dependency>
```

## Examples

The driver supports java 1.6 and up. But to truly get the benefit of the lambda support java 8 is recommended.

in java 8 one can do something Like

```java

RethinkDB r = RethinkDB.r;
RethinkDBConnection con = r.connect();

r.db("test").tableCreate("heros").run(con);
con.use("test");

r.table("heros").insert(
        new MapObject().with("name", "Heman").with("age", 33).with("skill", "sword"),
        new MapObject().with("name", "Spiderman").with("age", 27).with("skill", "jumping"),
        new MapObject().with("name", "Superman").with("age", 133).with("skill", "flying"),
        new MapObject().with("name", "Xena").with("age", 29).with("skill", "wowza")
).run(con);

System.out.println(
        r.table("heros").filter(hero->hero.field("age").gt(100)).pluck("age").run(con)
); // [{age=133.0}]

System.out.println(
        r.table("heros").orderBy(r.desc("age")).map(hero -> hero.field("name").upcase()).run(con)
); // [SUPERMAN, HEMAN, XENA, SPIDERMAN]

System.out.println(
        r.table("heros").update(row ->
                        r.branch(
                                row.field("age").gt(100),
                                new MapObject().with("newAttribute", "old"),
                                new MapObject().with("newAttribute", "young")
                        )
        ).run(con)
); // DMLResult{inserted=0, replaced=4, unchanged=0, errors=0, first_error=null, deleted=0, skipped=0, generated_keys=null, old_val=null, new_val=null}

System.out.println(
        r.table("heros").pluck("name","newAttribute").run(con)
); // [
   // {newAttribute=young, name=Xena},
   // {newAttribute=young, name=Spiderman},
   // {newAttribute=old,   name=Superman},
   // {newAttribute=young, name=Heman}
   // ]


```

See API Documentation and more examples here: [http://npiv.github.io/rethink-java-doc/html](http://npiv.github.io/rethink-java-doc/html/)

### Apache License 2.0
```
Copyright [2014] [Nick Verlinde]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
