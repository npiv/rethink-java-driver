import com.rethinkdb.RethinkDB;
import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.model.DBObjectBuilder;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.proto.RDatumBuilder;
import com.rethinkdb.proto.RTermBuilder;
import com.rethinkdb.query.option.Durability;

public class TT {

    private static RethinkDB r = RethinkDB.r;

    public static void main(String[] args) {
        RethinkDBConnection connection = r.connect("agillo.net");

//        connection.use("testTable");
//        System.out.println(r.dbCreate("test").run(connection));
//        System.out.println(r.dbList().run(connection));
//        System.out.println(r.dbDrop("toot").run(connection));

//        r.dbCreate("test2").run(connection);
//
//        System.out.println(connection.run(
//                Q2L.Query.newBuilder().setType(Q2L.Query.QueryType.START).setToken(123)
//                        .setQuery(Q2L.Term.newBuilder().setType(Q2L.Term.TermType.TABLE_LIST)
//
//                                .addArgs(
//                                        Q2L.Term.newBuilder().setType(Q2L.Term.TermType.DB)
//                                        .addArgs(RTermBuilder.datumTerm("test")).build()
//                                )
//
//
//
//
//        )));

        r.db("test2").tableDrop("test2").run(connection);
        System.out.println(r.db("test2").tableList().run(connection));


//        System.out.println(
//                r.insert(
//                        new DBObjectBuilder()
//                                .with("wow", "it works")
//                                .with("id", "9a1df541-0299-4040-a166-b989152f2335")
//                                .build(),
//                        Durability.hard,
//                        true,
//                        true
//                ).run(connection)
//        );
    }
}
